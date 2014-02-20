function success = del(relvar)
% dj.BaseRelvar.del() for UI applications

success = false;

if ~relvar.exists
    success = true;
else
    % compile the list of relvars to be deleted from
    list = relvar.table.descendants;
    tabs = cellfun(@(name) dj.Table(name), list, 'UniformOutput', false);
    tabs = [tabs{:}];
    rels = cellfun(@(name) init(dj.BaseRelvar, dj.Table(name)), list, 'UniformOutput', false);
    rels = [rels{:}];
    rels(1) = rels(1) & relvar.restrictions;
    
    % apply proper restrictions
    restrictByMe = arrayfun(@(tab) any(ismember(tab.references, list)), tabs);  % restrict by all association tables
    restrictByMe(1) = ~isempty(relvar.restrictions); % if relvar has restrictions, then restrict by relvar
    for i=1:length(rels)
        for ix = cellfun(@(child) find(strcmp(child,list)), [tabs(i).children tabs(i).referencing])
            if restrictByMe(i)
                rels(ix).restrict(pro(rels(i)));
            else
                rels(ix).restrict(rels(i).restrictions{:});
            end
        end
    end
    
    msg = sprintf('%s\n', 'ABOUT TO DELETE:');
    counts = nan(size(rels));
    for i=1:numel(rels)
        counts(i) = rels(i).count;
        if counts(i)
            msg = sprintf('%s\n%8d tuples from %s (%s)', msg, ...
                counts(i), tabs(i).fullTableName, ...
                tabs(i).info.tier);
        end
    end
    msg = sprintf('%s\n%s', msg, 'Continue?');
    sel = questdlg(msg, 'Delete tuples');
    if strcmpi(sel, 'yes')
        status = dj.set('suppressPrompt', true);
        hprog = waitbar(0.5, 'Deleting tuples');
        success = relvar.del();
        delete(hprog)
        dj.set('suppressPrompt', status);
    end
end
end
