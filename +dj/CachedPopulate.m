% dj.CachedPopulate is an abstract mixin class that allows a dj.Relvar object
% to automatically populate its table by pushing jobs to a cluster via the
% Parallel Computing Toolbox
%


classdef CachedPopulate < dj.BatchPopulate
    
    properties(Abstract)
        granularityRel     % specify the relation providing granularity of cache levels
    end

    methods
        function varargout = cached_populate(self, varargin)
            % dj.CachedPopulate/cached_populate in similar to
            % dj.BatchPopulate/batch_populate
            % except that data is cached on an SSD volume before
            % being processed on the cluster. One job is created per key.
            % Appropriate cache requests are added to storage.CacheRequests.
            % Execution of jobs is gated by boolean resource attributes
            %
            % The job reservation table <package>.Jobs required by parpopulate is also
            % used by cached_populate.
            % See also dj.AutoPopulate/parpopulate, dj.BatchPopulate/batch_populate
            
            self.schema.conn.cancelTransaction  % rollback any unfinished transaction
            self.useReservations = true;
            self.populateSanityChecks
            
            % if the last argument is a function handle, apply it to popRel.
            restr = varargin;
            unpopulated = self.popRel;
            if ~isempty(restr) && isa(restr{end}, 'function_handle')
                unpopulated = restr{end}(unpopulated);
                restr{end}=[];
            end
            % restrict the popRel to unpopulated tuples
            unpopulated = (unpopulated & restr) - self;

            sge = getDefaultScheduler;
            pathDeps = setPath();
            
            function j = init_cache_job(gran_key)
                [disk, path] = self.get_cache_request(gran_key);
                req_key = dj.CachedPopulate.get_cache_key(disk, path);
                
                % Assemble resource requirements:
                % boolean cache_XXXXXXXX attribute for gating
                % io_YYYYYY attribute to limit I/O load on caching server
                io_load = self.get_io_load();
                cr = {};
                if ~isempty(io_load) && (io_load > 0)
                    cr = {'io_serenity', io_load};
                end
                cr(end+1, 1:2) = {dj.CachedPopulate.get_cache_flag(req_key), 1};
                user_data.complexResource = cr;
                j = createJob(sge, batch_path_keyword(), pathDeps, ...
                    'UserData', user_data);
            end
            
            pregen_jobs = [];
            pregen_map = [];
            if exists(unpopulated)
                gran_keys = fetch(self.granularityRel & unpopulated);
                gran_hashes = arrayfun(@dj.DataHash, gran_keys, 'UniformOutput', false);
                gran_jobs = arrayfun(@init_cache_job, gran_keys, ...
                    'UniformOutput', false);
                pregen_jobs = [gran_jobs{:}];
                pregen_map = containers.Map(gran_hashes, num2cell(1:numel(gran_hashes)));
            end
            
            self.executionEngine = @(key, fun, args) ...
                cachedExecutionEngine(self, key, fun, args, pregen_jobs, pregen_map);
            [varargout{1:nargout}] = self.populate_(varargin{:});

            for job=pregen_jobs(:)'
                if numel(job.Tasks) > 0
                    submit(job);
                else
                    job.delete
                end
            end
        end
    end
    
    methods(Access = protected, Abstract)
        [request_drive, request_path] = get_cache_request(self, gran_key)
        % Return LD unit and directory that need to be cached
        % gran_key    - single key into granularityRel
    end

    methods(Access = protected)
        function io_slots = get_io_load(self, key) %#ok<INUSD>
            % Override this function as required.
            io_slots = 1;
        end
        
        
        function cachedExecutionEngine(self, key, fun, args, pregen_jobs, pregen_map)
            % Initiate caching of dataset
            gran_key = fetch(self.granularityRel & key);
            [disk, path] = self.get_cache_request(gran_key);
            req_key = dj.CachedPopulate.get_cache_key(disk, path);
            dj.CachedPopulate.increment_request_count(req_key);
            
            job_idx = pregen_map(dj.DataHash(gran_key));
            job = pregen_jobs(job_idx);
            createTask(job, fun, 0, args);
        end
        
        function postExecutionHook(self, key)
            [disk, path] = self.get_cache_request(key);
            req_key = fetch(storage.CacheRequests & struct( ...
                'disk_label', disk, 'request_path', path));
            if ~isempty(req_key)
                % Mark job as completed if cache was used for
                % this population
                dj.CachedPopulate.increment_completion_count(req_key)
            end
        end
    end
    
    
    methods (Static, Access=private)
        function req_key = get_cache_key(disk, path)
            hash = dj.DataHash(struct('disk_label', disk, ...
                'request_path', path));
            req_key = struct('request_hash', hash);
            if ~exists(storage.CacheRequests & req_key)
                % How much data needs to be cached?
                src_path = fullfile('/LD', disk, path);
                assert(logical(exist(src_path, 'dir')));
                [~, req_size] = system( sprintf('du -B 1 -s "%s" | cut -f 1', ...
                    src_path));
                req_size = sscanf(strtrim(req_size), '%lu');
                % This could take a while. Make sure there is still no req.
                % in the DB
                if ~exists(storage.CacheRequests & req_key)
                    % Create SGE resource
                    dj.CachedPopulate.init_sge_resource(req_key);
                    insert(storage.CacheRequests, struct( ...
                        'request_hash', hash, ...
                        'disk_label', disk, ...
                        'request_path', path, ...
                        'nb_clients', 0, ...
                        'request_size', req_size))
                end
            end
        end
        
        function init_sge_resource(req_key)
            % Create a boolean resource attribute used to halt
            % execution of the job until data is cached
            flag = dj.CachedPopulate.get_cache_flag(req_key);
            % Read complex config
            config_file = tempname;
            system(sprintf('qconf -sc > "%s"', config_file));
            temp_cleanup = onCleanup(@() delete(config_file));
            % Append new attribute
            fid = fopen(config_file, 'a');
            fprintf(fid, '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\n', ...
                flag, flag, 'BOOL', '==', 'YES', 'NO', 'FALSE', 0);
            fclose(fid);
            % Reload complex config
            system(sprintf('qconf -Mc "%s"', config_file));
        end
        
        function flag = get_cache_flag(req_key)
            % Map req_key to resource attribute name
            flag = sprintf('cache_%s', req_key.request_hash(1:8));
        end
    end
    
    methods(Static, Access=private)
        function increment_request_count(req_key)
            % Atomically increment the request count for a cache entry
            conn = storage.CacheRequests().schema.conn;
            conn.query(sprintf( ...
            ['UPDATE %s SET nb_clients=nb_clients+1 WHERE ' ...
             'request_hash="%s"'], ...
            storage.CacheRequests.table.fullTableName, req_key.request_hash))
        end
        
        function increment_completion_count(req_key)
            % Atomically increment the completion count for a cache entry
            conn = storage.CacheEntries().schema.conn;
            conn.query(sprintf( ...
            ['UPDATE %s SET fulfilled_requests=fulfilled_requests+1 WHERE ' ...
             'request_hash="%s"'], ...
            storage.CacheEntries.table.fullTableName, req_key.request_hash))
        end
    end
end
