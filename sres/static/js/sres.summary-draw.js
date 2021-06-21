function drawRepresentation(dataElement, presMode, chartElementId, data, options) {
    // store data and options
    if (typeof options === 'undefined') {
        options = dataElement.data('sres-chart-options');
    } else {
        dataElement.data('sres-chart-options', options);
    }
    if (typeof data === 'undefined') {
        data = dataElement.data('sres-chart-data');
    } else {
        dataElement.data('sres-chart-data', data);
    }
    // render
    let chart = undefined;
    switch (presMode) {
        case 'chart_column':
            chart = new google.charts.Bar(document.getElementById(chartElementId))
            chart.draw(data, google.charts.Bar.convertOptions(options));
            break;
        case 'chart_pie':
            chart = new google.visualization.PieChart(document.getElementById(chartElementId))
            chart.draw(data, options);
            break;
        case 'text':
            $('#' + chartElementId).html(data);
            break;
        case 'bullets':
            let listElements = [];
            data.forEach(function(item){
                listElements.push(item);
            });
            $('#' + chartElementId).html( '<ul><li>' + listElements.join('</li><li>') + '</li></ul>' );
            break;
        case 'wordcloud':
            $('#' + chartElementId).html('');
            $('#' + chartElementId).jQCloud(data, options);
            break;
    }
}

// Redraw on window resize
$(window).on('resize', function(){
    $('.sres-summary-representation-chart').each(function(){
        drawRepresentation(
            $(this),
            $(this).attr('data-sres-presentation-mode'),
            $(this).attr('id')
        );
    });
});

function updateSummaryRepresentation(summaryId, useCached, useIdAsIs) {
    let summaryCard = $(".sres-summary-card[id='" + summaryId + "']");
    let summaryChartId = summaryId + '_chart';
    if (typeof useIdAsIs !== 'undefined' && useIdAsIs == true) {
        summaryCard = $('#' + summaryId);
        summaryChartId = summaryId;
    }
    let presMode = summaryCard.attr('data-sres-presentation-mode');
    let calcMode = summaryCard.attr('data-sres-calculation-mode');
    let groupingMode = summaryCard.attr('data-sres-grouping-mode');
    let groupingComparisonMode = summaryCard.attr('data-sres-grouping-comparison-mode');
    let groupingColRef = summaryCard.attr('data-sres-grouping-column-reference');
    let groupingColVals = summaryCard.find('.sres-summary-grouping-values').val();
    let colRef = summaryCard.attr('data-sres-column-reference-encoded');
    let chartElement = undefined;
    
    if (typeof useCached !== 'undefined' && useCached === true) {
        chartElement = $('#' + summaryChartId);
        drawRepresentation(chartElement, presMode, summaryChartId);
    } else {
        
        // Check all bits are good before loading from server
        if (!(colRef && presMode && calcMode)) {
            return;
        }
        
        // load from server
        let url = ENV['GET_REPRESENTATION_DATA_ENDPOINT'];
        url = url.replace('__col__', colRef);
        url = url.replace('__calc_mode__', calcMode);
        url = url.replace('__pres_mode__', presMode);
        url = url.replace('__group_mode__', groupingMode);
        url = url.replace('__group_comp_mode__', groupingComparisonMode);
        url = url.replace('__group_col__', groupingColRef);
        url = url.replace('__group_vals__', encodeURIComponent(groupingColVals));
        
        // any extra config?
        let calcModeExtraConfig = summaryCard.attr('data-sres-calculation-mode-extra-config');
        try {
            calcModeExtraConfig = JSON.parse(calcModeExtraConfig);
        } catch(e) {
            calcModeExtraConfig = {};
        }
        Object.keys(calcModeExtraConfig).forEach(function(configId){
            url += '&calc_mode_' + configId + '=' + encodeURIComponent(calcModeExtraConfig[configId])
        });
        let presModeExtraConfig = summaryCard.attr('data-sres-presentation-mode-extra-config');
        try {
            presModeExtraConfig = JSON.parse(presModeExtraConfig);
        } catch(e) {
            presModeExtraConfig = {};
        }
        Object.keys(presModeExtraConfig).forEach(function(configId){
            url += '&pres_mode_' + configId + '=' + encodeURIComponent(presModeExtraConfig[configId])
        });
        
        let qName = 'sres_summary_get_representation_' + summaryId;
        $.ajaxq.abort(qName);
        $.ajaxq(qName, {
            url: url,
            method: 'GET',
            success: function(data){
                // parse return payload
                try {
                    data = JSON.parse(data);
                } catch(e) {
                    $.notify(
                        { message: 'There was an error generating the data for this summary. Double-check that the settings work for the data available.' },
                        { type: 'danger' }
                    );
                }
                // grouping
                let groupingSelect = summaryCard.find('.sres-summary-grouping-values');
                if (groupingSelect.find('option').length == 0 || groupingSelect.attr('data-sres-grouping-column-reference-current') != data.grouping_column_reference) {
                    if (data.possible_grouping_values.length > 0) {
                        summaryCard.find('.sres-summary-grouping-container').removeClass('d-none');
                        //summaryCard.find('.sres-summary-grouping-values').html('<option value="" selected>Everyone</option>');
                        groupingSelect.html('').trigger('chosen:updated');
                        data.possible_grouping_values.forEach(function(groupingValue) {
                            groupingSelect.append('<option value="' + encodeURIComponent(groupingValue) + '">' + Handlebars.escapeExpression(groupingValue) + '</option>');
                        });
                        groupingSelect.chosen({
                            width: '100%',
                            placeholder_text_multiple: 'Everyone, or choose option(s)'
                        }).trigger('chosen:updated');
                        summaryCard.find('.sres-summary-grouping-column-name').text(data.grouping_column_name);
                        groupingSelect.attr('data-sres-grouping-column-reference-current', data.grouping_column_reference);
                    } else {
                        summaryCard.find('.sres-summary-grouping-container').addClass('d-none');
                        groupingSelect.html('').trigger('chosen:updated');
                    }
                }
                // draw representation
                let options = {}
                switch (presMode) {
                    case 'chart_column':
                        if (summaryCard.attr('data-sres-grouping-comparison-mode') == 'enabled') {
                            options['legend'] = {
                                position: 'in'
                            }
                        } else {
                            options['legend'] = {
                                position: 'none'
                            }
                        }
                        options['vAxis'] = {
                            title: data.y_axis_label
                        }
                        if (data.x_axis_label) {
                            options['hAxis'] = {
                                title: data.x_axis_label
                            }
                        }
                    case 'chart_pie':
                        summaryCard.find('.sres-summary-card-body').html('<div id="' + summaryChartId + '" class="sres-summary-representation-chart" data-sres-presentation-mode="' + presMode + '"></div>');
                        chartElement = $('#' + summaryChartId);
                        let chartData = google.visualization.arrayToDataTable(data.data_array);
                        drawRepresentation(chartElement, presMode, summaryChartId, chartData, options)
                        break;
                    case 'text':
                        summaryCard.find('.sres-summary-card-body').html('<div id="' + summaryChartId + '" class="sres-summary-representation-text" data-sres-presentation-mode="' + presMode + '"></div>');
                        chartElement = $('#' + summaryChartId);
                        drawRepresentation(chartElement, presMode, summaryChartId, data.data_text, options)
                        break;
                    case 'bullets':
                        summaryCard.find('.sres-summary-card-body').html('<div id="' + summaryChartId + '" class="sres-summary-representation-text" data-sres-presentation-mode="' + presMode + '"></div>');
                        chartElement = $('#' + summaryChartId);
                        drawRepresentation(chartElement, presMode, summaryChartId, data.data_array, options)
                        break;
                    case 'wordcloud':
                        options['autoResize'] = true;
                        summaryCard.find('.sres-summary-card-body').html('<div id="' + summaryChartId + '" class="sres-summary-representation-wordcloud" style="width:100%; height:40vh;" data-sres-presentation-mode="' + presMode + '"></div>');
                        chartElement = $('#' + summaryChartId);
                        drawRepresentation(chartElement, presMode, summaryChartId, data.data_array, options)
                        break;
                }
            }
        });
    }
}

