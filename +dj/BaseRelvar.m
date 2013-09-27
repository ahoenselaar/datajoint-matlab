% dj.BaseRelvar - a relational variable linked to a table in the database.
% BaseRelvar provides data manipulation operators del and insert.
%
% SYNTAX:
%    obj = dj.BaseRelvar(table)  % table must be of class dj.Table

classdef BaseRelvar < dj.GeneralRelvar
    
    properties(Dependent,Access=private)
        tab     % associated table
    end
    
    properties(Dependent, SetAccess = private)
        lastInsertID  % Value of Last auto_incremented primary key 
    end
    
    methods
        function self = init(self, table)
            switch true
                case isa(table, 'dj.Table')
                    init@dj.GeneralRelvar(self, 'table', {table});
                case isa(table, 'dj.BaseRelvar')
                    init@dj.GeneralRelvar(self, 'table', {table.tab}, table.restrictions);
                otherwise
                    throwAsCaller(MException('BaseRelvar requires a dj.Table object'))
            end
        end
        
        
        function info = get.tab(self)
            info = self.operands{1};
        end
        
        
        function id = get.lastInsertID(self)
            % query MySQL for the last auto_incremented key
            ret = query(self.schema.conn, 'SELECT last_insert_id() as `lid`');
            id = ret.lid;
        end
      
        
        function delQuick(self)
            % dj.BaseRelvar/delQuick - remove all tuples of the relation from its table.
            % Unlike dj.BaseRelvar/del, delQuick does not prompt for user
            % confirmation, nor does it attempt to cascade down to the dependent tables.
            
            self.schema.conn.query(sprintf('DELETE FROM %s', self.sql))
        end
      
        
        
        function success = del(self, do_prompt)
            % dj.BaseRelvar/del - remove all tuples of relation self from its table
            % as well as all dependent tuples in dependent tables.
            %
            % A summary of the data to be removed will be provided followed by
            % an interactive confirmation before deleting the data.
            %
            % EXAMPLES:
            %   del(common.Scans) % delete all tuples from table Scans and all tuples in dependent tables.
            %   del(common.Scans & 'mouse_id=12') % delete all Scans for mouse 12
            %   del(common.Scans - tp.Cells)  % delete all tuples from table common.Scans 
            %                                   that do not have matching tuples in table Cells
            %
            % See also dj.BaseRelvar/delQuick, dj.Table/drop
            
            if nargin < 2
                do_prompt = true;
            end
            self.schema.conn.cancelTransaction  % exit ongoing transaction, if any
            success = false;
            
            if ~self.exists
                disp 'nothing to delete'
                success = true;
            else
                % warn the user if deleting from a subtable
                if do_prompt && ismember(self.tab.info.tier, {'imported','computed'}) ...
                        && ~isa(self, 'dj.AutoPopulate')
                    fprintf(['!!! %s is a subtable. For referential integrity, ' ...
                        'delete from its parent instead.\n'], class(self))
                    if ~strcmpi('yes', input('Prceed anyway? yes/no >','s'))
                        disp 'delete cancelled'
                        return
                    end
                end
                               
                % compile the list of relvars to be deleted
                disp 'ABOUT TO DELETE:'
                fprintf('%4d tuples from %s (%s)\n', ...
                    self.count, self.tab.fullTableName, self.tab.info.tier)

                names = {self.tab.className};
                rels = {self};
                new = struct('rel',self,'restrictByMe',~isempty(self.restrictions));
                while ~isempty(new)
                    curr = new(1);
                    new(1) = [];
                    sch = curr.rel.schema;
                    ixCurr = strcmp(sch.classNames, curr.rel.tab.className);
                    j = find(sch.dependencies(:,ixCurr));
                    j = j(~ismember(sch.classNames(j),names));  % remove duplicates
                    j = j(:)';  
                    primary = full(sch.dependencies(j,ixCurr))==1;
                    children = sch.classNames(j);                
                    names = [names children]; %#ok<AGROW>
                    for j=1:length(children)
                        child = sch.conn.getPackage(children{j},false);
                        if child(1) == '$'  % ignore unloaded schemas
                            warning('Ignoring %s because its schema is not loaded.', child)
                        else
                            if curr.restrictByMe
                                rel = init(dj.BaseRelvar, dj.Table(child)) & curr.rel;
                            else
                                rel = init(dj.BaseRelvar, dj.Table(child)) & curr.rel.restrictions;
                            end
                            n = rel.count;
                            if n
                                fprintf('%4d tuples from %s (%s)\n', ...
                                    n, rel.tab.fullTableName, rel.tab.info.tier)
                                rels{end+1} = rel; %#ok<AGROW>
                                new(end+1) = struct('rel', rel,'restrictByMe', ~primary(j)); %#ok<AGROW>
                            end
                        end
                    end
                end
                
                % confirm and delete
                if do_prompt && ~strcmpi('yes', input('Proceed to delete? yes/no >', 's'))
                    disp 'delete canceled'
                else
                    self.schema.conn.startTransaction
                    try
                        for iRel = length(rels):-1:1
                            fprintf('Deleting from %s... ', rels{iRel}.tab.className)
                            self.schema.conn.query(sprintf('DELETE FROM %s%s', ...
                                rels{iRel}.tab.fullTableName, rels{iRel}.whereClause))
                            fprintf '(not committed)\n'
                        end
                        self.schema.conn.commitTransaction
                        fprintf ' ** delete committed\n'
                        success = true;
                    catch err
                        fprintf '\n ** delete rolled back due to to error\n'
                        self.schema.conn.cancelTransaction
                        rethrow(err)
                    end
                end
            end
        end
        
        
        function validateDecimalData(self, tuples, rel_err_tol)
            % validateDecimalData(self, tuples, [rel_err_tol=Inf])
            % Check given tuples will convert correctly to decimal datatype
            %
            % Only checks decimal datatype fields. The optional argument
            % will check that the relative error induced by conversion
            % falls below a specified bound.
            if nargin < 3
                rel_err_tol = Inf;
            end
            header = self.tab.header;
            % Restrict to decimal datatype cols present in the given tuples
            is_decimal = strncmpi({header.type},'decimal',7);
            is_given = ismember({header.name},fieldnames(tuples));
            for col_idx = find(is_decimal & is_given)
                % Parse the decimal value range from the datatype
                type_spec  = header(col_idx).type;
                prec_scale = sscanf(type_spec, 'decimal(%d,%d)');
                prec   = prec_scale(1);
                scale  = prec_scale(2);
                maxval = 10^(prec-scale) - 10^(-scale);
                % Check that they fall within range
                col_name = header(col_idx).name;
                vals = [tuples.(col_name)];
                assert( all(abs( vals ) <= maxval), ...
                    'dj:validateDecimalData:OutOfRange', ...
                    'Values for field "%s" out of range', col_name );
                % Check that they meet conversion accuracy
                ulp = 10^(-scale);
                rounded = round(vals / ulp) * ulp;
                rel_err = (rounded - vals) ./ vals;
                nondiv0 = vals ~= 0;
                assert( all(abs(rel_err(nondiv0)) < rel_err_tol), ...
                    'dj:validateDecimalData:PoorAccuracy', ...
                    'Values for field "%s" have excessive roundoff error',...
                    col_name );
                % Go on to the next column
            end
            % Done!
        end
        
        function insert(self, tuples, command)
            % insert an array of tuples directly into the table
            %
            % The input argument tuples must a structure array with field
            % names exactly matching those in the table.
            %
            % The optional argument 'command' allows replacing the MySQL
            % command from the default INSERT to INSERT IGNORE or REPLACE.
            %
            % Duplicates, unmatched attributes, or missing required attributes will
            % cause an error, unless command is specified.
            
            assert(isstruct(tuples), 'Tuples must be a non-empty structure array')
            if isempty(tuples)
                return
            end
            if nargin<=2
                command = 'INSERT';
            end
            assert(any(strcmpi(command,{'INSERT', 'INSERT IGNORE', 'REPLACE'})), ...
                'invalid insert command')
            header = self.tab.header;
            
            % validate header
            fnames = fieldnames(tuples);
            found = ismember(fnames,{header.name});
            if ~all(found)
                error('Field %s is not found in the table %s', ...
                    fnames{find(~found,1,'first')}, class(self));
            end
            
            % Validate decimal datatype value ranges
            validateDecimalData(self, tuples);
            
            % Cache some values for efficiency
            full_tab_name = self.tab.fullTableName;
            connection = self.schema.conn;
            
            % form query
            ix = ismember({header.name}, fnames);
            for tuple=tuples(:)'
                queryStr = '';
                blobs = {};
                for i = find(ix)
                    v = tuple.(header(i).name);
                    if header(i).isString
                        assert(ischar(v), ...
                            'The field %s must be a character string', ...
                            header(i).name)
                        if isempty(v)
                            queryStr = sprintf('%s`%s`="",', ...
                                queryStr, header(i).name);
                        else
                            queryStr = sprintf('%s`%s`="{S}",', ...
                                queryStr,header(i).name);
                            blobs{end+1} = v;  %#ok<AGROW>
                        end
                    elseif header(i).isBlob
                        queryStr = sprintf('%s`%s`="{M}",', ...
                            queryStr,header(i).name);
                        if islogical(v) % mym doesn't accept logicals - save as uint8 instead
                            v = uint8(v);
                        end
                        blobs{end+1} = v;    %#ok<AGROW>
                    else
                        if islogical(v)  % mym doesn't accept logicals - save as unit8 instead
                            v = uint8(v);
                        end
                        assert((isscalar(v) && isnumeric(v)) || isempty(v),...
                            'The field %s must be a numeric scalar value', ...
                            header(i).name)
                        if ~isnan(v)  % nans are not passed: assumed missing.
                            if strcmp(header(i).type, 'bigint')
                                queryStr = sprintf('%s`%s`=%d,',...
                                    queryStr, header(i).name, v);
                            elseif strcmp(header(i).type, 'bigint unsigned')
                                queryStr = sprintf('%s`%s`=%u,',...
                                    queryStr, header(i).name, v);
                            elseif isempty(v) && header(i).isautoincrement
                                queryStr = sprintf('%s`%s`=NULL,',...
                                    queryStr, header(i).name);
                            elseif ~isempty(v)  % nans are not passed: assumed missing.
                                queryStr = sprintf('%s`%s`=%1.16g,',...
                                    queryStr, header(i).name, v);
                            end
                        end
                    end
                end
                
                % issue query
                connection.query( sprintf('%s %s SET %s', ...
                    command, full_tab_name, ...
                    queryStr(1:end-1)), blobs{:})
            end
        end
        
        
        function inserti(self, tuples)
            % insert tuples but ignore errors. This is useful for rare
            % applications when duplicate entries should be quitely
            % discarded, for example.
            self.insert(tuples, 'INSERT IGNORE')
        end
        
        
        function update(self, attrname, value)
            % dj.BaseRelvar/update - update a field in an existing tuple
            %
            % Relational database maintain referential integrity on the level
            % of a tuple. Therefore, the UPDATE operator can violate referential
            % integrity and should not be used routinely.  The proper way
            % to update information is to delete the entire tuple and
            % insert the entire update tuple.
            %
            % Safety constraints:
            %    1. self must be restricted to exactly one tuple
            %    2. the update attribute must not be in primary key
            %
            % EXAMPLES:
            %   update(v2p.Mice & key, 'mouse_dob',   '2011-01-01')
            %   update(v2p.Scan & key, 'lens')   % set the value to NULL
            
            assert(count(self)==1, 'Update is only allowed on one tuple at a time')
            isNull = nargin<3;
            header = self.header;
            ix = find(strcmp(attrname,{header.name}));
            assert(numel(ix)==1, 'invalid attribute name')
            assert(~header(ix).iskey, 'cannot update a key value. Use insert(..,''REPLACE'') instead')
            
            switch true
                case isNull
                    queryStr = 'NULL';
                    value = {};
                    
                case header(ix).isString
                    assert(ischar(value), 'Value must be a string')
                    queryStr = '"{S}"';
                    value = {value};
                case header(ix).isBlob
                    if isempty(value) && header(ix).isnullable
                        queryStr = 'NULL';
                        value = {};
                    else
                        queryStr = '"{M}"';
                        if islogical(value)
                            value = uint8(value);
                        end
                        value = {value};
                    end
                case header(ix).isNumeric
                    if islogical(value)
                        value = uint8(value);
                    end
                    assert(isscalar(value) && isnumeric(value), 'Numeric value must be scalar')
                    if isnan(value)
                        assert(header(ix).isnullable, ...
                            'attribute `%s` is not nullable. NaNs not allowed', attrname)
                        queryStr = 'NULL';
                        value = {};
                    else
                        queryStr = sprintf('%1.16g',value);
                        value = {};
                    end
                otherwise
                    error 'Invalid condition: please report to DataJoint developers'
            end
            
            queryStr = sprintf('UPDATE %s SET `%s`=%s %s', ...
                self.tab.fullTableName, attrname, queryStr, self.whereClause);
            self.schema.conn.query(queryStr, value{:})
        end
    end
end
