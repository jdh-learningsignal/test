var queryBuilderOpts = {  
    plugins: {
        'not-group': {
            icon_unchecked: 'fa fa-square',
            icon_checked: 'fa fa-check-square'
        },
        'chosen-selectpicker': {
            search_contains: true,
            width: '100%'
        }
    },
    icons: {
        add_group: 'fa fa-plus',  
        add_rule: 'fa fa-plus',  
        remove_group: 'fa fa-trash',  
        remove_rule: 'fa fa-trash',  
        error: 'fa fa-exclamation-triangle',  
    },
    allow_empty: true
}

// changing lists for column selectors
$(document).on('sres.listchooserclosed', function(event, args){
    $.notify({message: 'Loading columns...'});
    args['lists'].forEach(function(list){
        $.ajax({
            url: ENV['LIST_COLUMNS_ENDPOINT'].replace('__table_uuid__', list),
            success: function(data) {
                data = JSON.parse(data);
                let disabled = "";
                let newFilters = [];
                var existingFilterColumnReferences = queryBuilderFilters.map(x => x.id);
                for (var c = 0; c < data.columns.length; c++) {
                    //console.log(c, data.columns[c]);
                    if (data.columns[c]['type'] == 'label-only') { 
                        disabled = ' disabled ';
                    } else {
                        if (existingFilterColumnReferences.indexOf(data.columns[c]['value']) == -1){
                            //console.log('pushing');
                            newFilters.push({
                                id: data.columns[c]['value'],
                                label: data.columns[c]['full_display_text'],
                                type: 'string'
                            });
                        }
                    }
                }
                $.notify({message: 'Adding columns...'});
                // Add to queryBuilder
                $("div.sres-querybuilder-container:visible").each(function(){
                    //console.log('newFilters', newFilters);
                    $(this).queryBuilder('addFilter', newFilters);
                });
                // Add to simple select
                newFilters.forEach(function(newFilter){
                    if ($(".sres-multiselect-dynamic-column-receiver").length) {
                        $(".sres-multiselect-dynamic-column-receiver").multiSelect('addOption', {
                            value: newFilter.id,
                            text: newFilter.label
                        });
                    }
                });
                // Add to var queryBuilderFilters
                queryBuilderFilters = queryBuilderFilters.concat(newFilters);
            }
        });
    });
});

/**
    Retaining set value when changing filter
**/
queryBuilderPreviousMeta = {};
$(document).on('change', '.rule-value-container input, .rule-operator-container select', function(event){
    // store rule value
    let container = $(this).parents('.rule-container');
    let containerId = container.attr('id');
    let value = $(this).val();
    let inputName = $(this).attr('name');
    if (!queryBuilderPreviousMeta.hasOwnProperty(containerId)) {
        queryBuilderPreviousMeta[containerId] = {
            operator: null,
            data: {}
        }
    }
    queryBuilderPreviousMeta[containerId]['operator'] = container.find('.rule-operator-container select').val();
    queryBuilderPreviousMeta[containerId]['data'][inputName] = value;
    //console.log('storing', inputName, value, queryBuilderPreviousMeta[containerId]);
});
$(document).ready(function(){
    $('.rule-value-container input').trigger('change');
    $('.rule-operator-container select').trigger('change');
    $('.sres-querybuilder-container')
        .on('afterUpdateRuleFilter.queryBuilder', function(e, r){
            let containerId = r.$el.attr('id');
            if (queryBuilderPreviousMeta.hasOwnProperty(containerId)) {
                r.$el.find('.rule-operator-container select').val(queryBuilderPreviousMeta[containerId]['operator']).trigger('change');
                Object.keys(queryBuilderPreviousMeta[containerId]['data']).forEach(function(key, index){
                    //console.log('gotakey', key, '-->', queryBuilderPreviousMeta[containerId]['data'][key]);
                    r.$el.find('.rule-value-container input[name=' + key + ']').val(queryBuilderPreviousMeta[containerId]['data'][key]).trigger('change');
                });
            }
        });
});