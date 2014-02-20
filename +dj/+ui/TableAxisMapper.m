classdef TableAxisMapper < handle
    
    properties
        fields = {'*'};     % Cell-array of fields to fetch from the relvar
        display_fields = {};% Additional fields to fetch, e.g. to create labels
        sorter = @(x) x;    % Sorting function for retrieved struct array
        mapper              % Function that extracts a unique numeric or
                            % string ID for each struct array element
        captions = @(x) []; % Function that turns struct array into
                            % cell array of caption strings
    end
    
    properties (Access=protected)
        map
        sortedData          % We store this in order to be able to
                            % reverse map table selection information
    end
    
    methods
        function self=TableAxisMapper(varargin)
            p = inputParser;
            p.addParamValue('fields', {'*'}, @iscell);
            p.addParamValue('display_fields', {}, @iscell);
            p.addParamValue('sorter', @(x) x, ...
                @(x) isa(x, 'function_handle'));
            p.addParamValue('mapper', ...
                dj.ui.field_selector('label'), ...
                @(x) isa(x, 'function_handle'));
            p.addParamValue('captions', ...
                dj.ui.field_selector('label'), ...
                @(x) isa(x, 'function_handle'));
            p.parse(varargin{:});
            params = p.Results;
            % Copy into properties
            param_fields = fieldnames(params);
            for fn=param_fields(:)'
               self.(fn{1}) = params.(fn{1}); 
            end
        end
        
        function labels = fetch(self, base_rel)
            if isa(base_rel, 'dj.GeneralRelvar')
                data = fetch(base_rel, self.fields{:}, self.display_fields{:});
            else
                if any(strcmp('*', self.fields))
                    data = base_rel;
                else
                    data = dj.struct.pro(base_rel, self.fields{:}, self.display_fields{:});
                end
            end
            % Apply sorting
            data = self.sorter(data);
            % Create internal mapping from keys to linear index
            mapped_data = self.mapper(data);
            self.map = dj.ui.TableAxisMapper.create_map(mapped_data);
            self.sortedData = data;
            labels = self.captions(data);
        end
        
        function idx = map_tuples(self, tuples)
            mapped_data = self.mapper(tuples);
            idx = self.map(mapped_data);
        end
        
        function keys = reverse_map_indices(self, indices)
            keys = self.sortedData(indices);
        end
    end
    
    methods (Access=protected)
        
    end
    
    methods (Static, Access=private)
        function map = create_map(mapped_data)
            if isnumeric(mapped_data)
                if numel(mapped_data) > 1
                    map = griddedInterpolant( ...
                        mapped_data, 1:numel(mapped_data), 'nearest');
                else
                    map = @(x) mapped_data * ones(size(x));
                end
            elseif iscell(mapped_data) && all(cellfun(@ischar, mapped_data))
                % Use a patricia tree for efficient String -> int mapping
                patricia_tree = dj.ui.TableAxisMapper.create_patricia_instance();
                patricia_tree.set(mapped_data, 1:numel(mapped_data));
                map = @(x) double(patricia_tree.get(x));
            end
        end
        
        function tree_instance = create_patricia_instance()
            if ~exist('siapaslab.collections.StringIntMap', 'class')
                ml_base = fileparts(which('ML_BASE.'));
                javaaddpath(fullfile(ml_base, 'java'));
                javaaddpath(fullfile(ml_base, 'java', 'jars', 'patricia-trie-0.6.jar'));
            end
            tree_instance = siapaslab.collections.StringIntMap();
        end
    end
end     