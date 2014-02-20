function f=field_selector(field_name)
    f = @(x) selfun(x, field_name);
end


function sel = selfun(x, field_name)
    if isnumeric(x(1).(field_name))
        sel = [x.(field_name)]; 
    else
        sel = {x.(field_name)}; 
    end
end