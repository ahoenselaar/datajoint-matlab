% TableView     UI for tabular display of data in RelVars

classdef TableView < handle
    
    properties
        colRel                % Relvar for columns
        colMapper             % TableAxisMapper for columns
        rowRel                % Relvar for rows
        rowMapper             % TableAxisMapper for rows
        jointRel              % Relvar populated by colRel AND rowRel
        contentProvider       % Callback that supplies display data to the TableView
        selectionCallback     % Callback that receives cell selection change notifications
                              % Prototype: f(tableview, row_keys, col_keys)
        userData              % Allow user to assocaite data with the TableView
    end
    
    properties (Access=protected)
        table                   % Table handle
        fig                     % Figure handle
        buttons                 % Custom button handles
        buttonData              % Metadata for buttons
        tableContent            % Cell array with table data
    end
    
    methods
        function self = TableView(varargin)
            p = inputParser;
            p.addParamValue('colRel', struct([]), ...
                @(x) isa(x, 'dj.GeneralRelvar') || isstruct(x));
            p.addParamValue('rowRel', struct([]), ...
                @(x) isa(x, 'dj.GeneralRelvar') || isstruct(x));
            p.addParamValue('colMapper', []);
            p.addParamValue('rowMapper', []);
            p.addParamValue('jointRel', struct([]), ...
                @(x) isa(x, 'dj.GeneralRelvar') || isstruct(x));
            p.addParamValue('contentProvider', ...
                @default_content, ...
                @(x) isa(x, 'function_handle'));
            p.addParamValue('selectionCallback', ...
                @(t,r,c) [], ...
                @(x) isa(x, 'function_handle'));
            p.parse(varargin{:});
            params = p.Results;
            % Copy into properties
            param_fields = fieldnames(params);
            for fn=param_fields(:)'
                self.(fn{1}) = params.(fn{1});
            end
        end
        
        function id=add_button(self, caption, callback, enabled)
            % Add a user-defined button to the UI
            % caption  -  button caption [string]
            % callback - Callback when the button is clicked [function handle]
            %            Prototype: f(table_view)
            % enabled  - State of button [bool], enabled by default
            if nargin < 3
                enabled = true;
            end
            self.buttonData = [self.buttonData; {caption, callback, enabled}];
            id = size(self.buttonData,1);
        end
        
        function enable_buttons(self, id)
            % Enables the buttons with identifier 'id'.
            % This is the id that was initially returned by
            % add_button(...)
            self.buttonData(:,3) = {true};
            if ~isempty(self.buttons)
                set(self.buttons(id), 'Enable', 'on');
            end
        end
        
        function disable_buttons(self, id)
            % Disables (grays out) the buttons with identifier 'id'.
            % This is the id that was initially returned by
            % add_button(...)
            self.buttonData(:,3) = {false};
            if ~isempty(self.buttons)
                set(self.buttons(id), 'Enable', 'off');
            end
        end
        
        function ok_button = show(self, varargin)
            % Displays the dialog and returns true if OK was hit
            % and false in the case of "Cancel"
            
            % Get labels
            row_headers = self.rowMapper.fetch(self.rowRel);
            col_headers = self.colMapper.fetch(self.colRel);
            % Initialize table content
            self.tableContent = cell(numel(row_headers), numel(col_headers));
            self.contentProvider(self);
            % Create figure window and table control
            self.fig = figure('Units', 'normalized', ...
                'Position'  , [0.1 0.1 0.5 0.4], ...
                'MenuBar'   , 'none', varargin{:});
            self.table = uitable(self.fig, 'Units', 'normalized', ...
                'Position'  , [0.0 0.15 1.0 0.85], ...
                'CellSelectionCallback', ...
                @(src, event) cell_selection_callback(self, src, event), ...
                'ColumnName', col_headers, ...
                'RowName'   , row_headers, ...
                'Data'      , self.tableContent);
            
            % OK, Cancel and user-defined buttons
            uicontrol(self.fig,              ...
                'Units'      ,'normalized', ...
                'Position'   ,[ 0.05 0.0125 0.4 0.05], ...
                'String'     ,getString(message('MATLAB:uistring:popupdialogs:OK')), ...
                'Callback'   ,@(obj, ev) default_button_callback(self, obj, ev), ...
                'Tag'        ,'OK', ...
                'UserData'   ,'OK');
            
            uicontrol(self.fig, ...
                'Units'      ,'normalized', ...
                'Position'   ,[ 0.55 0.0125 0.4 0.05], ...
                'String'     ,getString(message('MATLAB:uistring:popupdialogs:Cancel')), ...
                'Callback'   ,@(obj, ev) default_button_callback(self, obj, ev), ...
                'Tag'        ,'Cancel', ...
                'UserData'   ,'Cancel');
            self.create_custom_buttons()
            
            % HACK: Get adaptive column widths exploiting java controls
            jscrollpane = findjobj(self.table);
            iters = 0;
            while ~isa(jscrollpane, ...
                    'javahandle_withcallbacks.com.mathworks.hg.peer.utils.UIScrollPane')
                drawnow
                pause(0.05)
                jscrollpane = findjobj(self.table);
                iters = iters + 1;
                assert(iters < 1000, 'Cannot obtain Java handle to UI object.'); 
            end
            jtable = jscrollpane.getViewport.getView;
            %jtable.setAutoResizeMode(jtable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
            jtable.setAutoResizeMode(jtable.AUTO_RESIZE_OFF);
            
            uiwait(self.fig);
            if ishandle(self.fig)
                ok_button = strcmp(get(self.fig, 'UserData'), 'OK');
                delete(self.fig);
            else
                ok_button = false;
            end
        end
        
        function set_data(self, tuples, field_name)
            % Fill table content from the struct array tuples.
            % Cell content is taken from the field "field_name" ["content"
            % by default]. The tuples need to include the fields
            % required by colMapper and rowMapper to map keys to rows
            % and columns
            if nargin < 3
                field_name = 'content';
            end
            if isempty(tuples)
                return
            end
            row_idx = self.rowMapper.map_tuples(tuples);
            col_idx = self.colMapper.map_tuples(tuples);
            lin_idx = sub2ind(size(self.tableContent), ...
                row_idx(:), col_idx(:));
            self.tableContent(lin_idx) = {tuples.(field_name)};
        end
        
        function update_view(self)
            set(self.table, 'Data', self.tableContent);
        end
    end
    
    methods (Access=protected)
        function default_content(self)
            % The default content provider.
            % It will extract the "content" field from the joint Relvar
            content_tuples = fetch(self.jointRel, 'content', ...
                self.colRel.fields{:}, self.rowRel.fields{:});
            self.set_data(content_tuples);
        end
        
        function default_button_callback(self, obj, ev)
            % Callback handler for "OK" and "Cancel"
            set(gcbf, 'UserData', get(obj, 'UserData'));
            uiresume(gcbf);
        end
        
        function cell_selection_callback(self, ~, event)
            % Callback for cell selection in the cluster table
            
            rows = event.Indices(:, 1);
            cols = event.Indices(:, 2);
            % Convert to keys
            if ~isempty(rows)
                row_keys = self.rowMapper.reverse_map_indices(rows);
            else
                row_keys = [];
            end
            if ~isempty(cols)
                col_keys = self.colMapper.reverse_map_indices(cols);
            else
                col_keys = [];
            end
            self.selectionCallback(self, row_keys, col_keys);
        end
        
        function create_custom_buttons(self)
            % Adds additional buttons to the UI as requested by the user
            % through add_button() calls
            nb_buttons = size(self.buttonData,1);
            if nb_buttons < 1
                self.buttons = [];
                return
            end
            
            % Calculate layout parameters
            if nb_buttons > 1
                button_width = 0.9 / (nb_buttons+1);
                button_gap = button_width / (nb_buttons-1);
            else
                button_width = 0.9;
                button_gap = 0;
            end
            % Create buttons
            on_off_sel = {'off', 'on'};
            for i=1:nb_buttons
                self.buttons(i) = uicontrol(self.fig,              ...
                    'Units'      ,'normalized', ...
                    'Position'   ,[ 0.05 + (i-1)*(button_width+button_gap), 0.075, ...
                                    button_width, 0.05], ...
                    'String'     , self.buttonData{i,1}, ...
                    'Callback'   , @(src,evt) self.buttonData{i,2}(self), ...
                    'Enable'     , on_off_sel{double(self.buttonData{i,3})+1});
            end
        end
    end
end