% dj.BatchPopulate is an abstract mixin class that allows a dj.Relvar object
% to automatically populate its table by pushing jobs to a cluster via the
% Parallel Computing Toolbox
%


classdef BatchPopulate < dj.AutoPopulate
    
    methods
        function varargout = batch_populate(self, varargin)
            % dj.BatchPopulate/batch_populate works identically to dj.AutoPopulate/parpopulate
            % except that it spools jobs to the cluster. It creates one job per key,
            % so using batch_populate only makes sense for long-running calculations.
            %
            % The job reservation table <package>.Jobs required by parpopulate is also
            % used by batch_populate.
            % See also dj.AutoPopulate/parpopulate
            
            % perform error checks
            self.populateSanityChecks
            self.schema.conn.cancelTransaction  % rollback any unfinished transaction
            self.useReservations = true;
            self.executionEngine = @(key, fun, args) ...
                batchExecutionEngine(self, key, fun, args);
            
            [varargout{1:nargout}] = self.populate_(varargin{:});
        end
    end
    
    
    methods(Access = protected)
        function user_data = get_job_user_data(self, key) %#ok<MANU,INUSD>
            % Override this function in order to customize the UserData argument
            % for job creation. This can be used to supply the io_resource field
            % for example.
            user_data = struct();
        end
        
        function batchExecutionEngine(self, key, fun, args)
            % getDefaultScheduler returns an instance of the job scheduler we
            % use in the lab.
            % For general use, replace with
            %   sge = parcluster;
            % or
            %   sge = parcluster('profile_name')
            % for some appropriate profile that has been created.
            
            sge = getDefaultScheduler();
            
            % setPath() returns a cell array with all aditional MATLAB
            % paths on the shared file system that should be passed on to
            % the worker
            pathDeps = setPath();
            j = createJob(sge, batch_path_keyword(), pathDeps, ...
                'UserData', self.get_job_user_data(key));
            createTask(j, fun, 0, args);
            submit(j);
        end
    end
end
