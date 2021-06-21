function updateRangeSliderDataFrom(columnUuid, subfield, identifier, setValue) {
    //console.log('updateRangeSliderDataFrom', columnUuid, subfield, identifier, setValue);
    let sliderConfig = ENV['rangeslider_config'][columnUuid][subfield];
    let sourceElement = $(".js-range-slider[data-sres-columnuuid=" + columnUuid + "][data-sres-subfield=" + subfield + '][data-sres-identifier="' + identifier + '"]');
    // set the value that counts
    sourceElement.attr('data-sres-value', setValue);
    // render
    if (sliderConfig['slider_mode'] == 'textual') {
        let fromValueIndex = sliderConfig['values'].indexOf(setValue);
        sourceElement.attr('data-from', fromValueIndex);
        return fromValueIndex;
    } else {
        sourceElement.attr('data-from', setValue);
        return setValue;
    }
}

function updateRangeSlider(columnUuid, subfield, identifier, setValue, doNotTriggerDirty) {
    //console.log('updateRangeSlider', columnUuid, subfield, identifier, setValue);
    let sliderConfig = ENV['rangeslider_config'][columnUuid][subfield];
    let sourceElement = $(".js-range-slider[data-sres-columnuuid=" + columnUuid + "][data-sres-subfield=" + subfield + '][data-sres-identifier="' + identifier + '"]');
    // set the value to the correct form depending on slider_mode
    setValue = updateRangeSliderDataFrom(columnUuid, subfield, identifier, setValue);
    // render
    let slider = sourceElement.data('ionRangeSlider');
    if (typeof slider == 'undefined') {
        //console.log('rangeslider not yet defined!');
        renderRangeSlider(columnUuid, subfield, identifier);
    } else {
        try {
            if (doNotTriggerDirty == true && !sourceElement.hasClass('sres-ignore-dirty')) {
                sourceElement.addClass('sres-ignore-dirty');
                slider.update({from: setValue}); // note this triggers change event in associated input
                sourceElement.removeClass('sres-ignore-dirty');
            } else {
                slider.update({from: setValue}); // note this triggers change event in associated input
            }
            slider.destroy();
            renderRangeSlider(columnUuid, subfield, identifier);
        } catch(e) {
            console.error(e);
        }
    }
}

function renderRangeSliderBadges(data) {
    let html = '';
    let left = 0;
    let i, values, min, max;
    let columnUuid = data.input.attr('data-sres-columnuuid');
    let a = data.input.attr('data-sres-subfield');
    let identifier = data.input.attr('data-sres-identifier');
    let sliderConfig = ENV['rangeslider_config'][columnUuid][a];
    if (sliderConfig['slider_mode'] == 'textual') {
        min = 0;
        max = 100;
        interval = max / (sliderConfig['labels'].length - 1);
        values = [];
        for (let v = 0; v < sliderConfig['labels'].length; v++) {
            values.push(interval * v);
        }
    } else {
        values = sliderConfig['values'];
        min = sliderConfig['values'][0];
        max = sliderConfig['values'][sliderConfig['values'].length - 1];
    }
    let labels = sliderConfig['labels'];
    let descriptions = sliderConfig['descriptions'];
    for (i = 0; i < values.length; i++) {
        if (i == 0) {
            html += '<span class="badge badge-light sres-entry-slider-label" data-sres-value="' + values[i] + '" data-tippy-arrow="true" data-tippy-placement="bottom" data-tippy-content="' + he.escape(descriptions[i]) + '" style="display:block; top:55px; position:absolute; left:0;"><span class="fa fa-caret-up"></span> ' + he.escape(labels[i]) + '</span>';
        } else if (i == values.length - 1) {
            html += '<span class="badge badge-light sres-entry-slider-label" data-sres-value="' + values[i] + '" data-tippy-arrow="true" data-tippy-placement="bottom" data-tippy-content="' + he.escape(descriptions[i]) + '" style="display:block; top:55px; position:absolute; right:0;">' + he.escape(labels[i]) + ' <span class="fa fa-caret-up"></span></span>';
        } else {
            left = (values[i] - min) / (max - min) * 100;
            html += '<span class="badge badge-light sres-entry-slider-label" data-sres-value="' + values[i] + '" data-tippy-arrow="true" data-tippy-placement="bottom" data-tippy-content="' + he.escape(descriptions[i]) + '" style="display:block; top:55px; margin-left:-5px; position:absolute; left:' + left + '%;"><span class="fa fa-caret-up"></span> ' + he.escape(labels[i]) + '</span>';
        }   
    }
    data.slider.append(html);
    refreshTooltips();
}

function renderRangeSlider(columnUuid, a, identifier) {
    //console.log('renderRangeSlider', columnUuid, a, identifier);
    let sourceElements = null;
    if (typeof a == 'undefined') {
        sourceElements = $(".js-range-slider[data-sres-columnuuid=" + columnUuid + "]");
    } else if (typeof identifier == 'undefined') {
        sourceElements = $(".js-range-slider[data-sres-columnuuid=" + columnUuid + "][data-sres-subfield=" + a + "]");
    } else {
        sourceElements = $(".js-range-slider[data-sres-columnuuid=" + columnUuid + "][data-sres-subfield=" + a + '][data-sres-identifier="' + identifier + '"]');
    }
    sourceElements.each(function(i, sourceElement) {
        sourceElement = $(sourceElement);
        if (typeof a == 'undefined') {
            a = sourceElement.attr('data-sres-subfield');
        }
        let sliderConfig = ENV['rangeslider_config'][columnUuid][a];
        let identifier = sourceElement.attr('data-sres-identifier');
        // Display misconfiguration warning if needed
        if (sliderConfig['values'].length == 0) {
            sourceElement.siblings('.sres-slider-misconfiguration-warning').removeClass('d-none');
        }
        // Render
        //console.log(i, 'renderingXX', columnUuid, a, identifier, sourceElement, sourceElement.attr('data-from'));
        if (sliderConfig['slider_mode'] == 'textual') {
            // Set 'from' value for textual sliders
            let data = sourceElement.attr('data-sres-value').toString();
            if (data) {
                updateRangeSliderDataFrom(columnUuid, a, identifier, data)
            } else {
                sourceElement.attr('data-sres-value', ENV['rangeslider_config'][columnUuid][a]['values'][0]);
            }
            // make the ionRangeSlider
            sourceElement.ionRangeSlider({
                type: 'single',
                values: sliderConfig['labels'],
                grid: true,
                //grid_snap: true,
                grid_num: 1,
                onChange: function(data) {
                    let columnUuid = data.input.attr('data-sres-columnuuid');
                    let a = data.input.attr('data-sres-subfield');
                    let identifier = data.input.attr('data-sres-identifier');
                    let sliderConfig = ENV['rangeslider_config'][columnUuid][a];
                    let value = sliderConfig['values'][data.from];
                    //console.log(sliderConfig, data.from);
                    data.input.attr('data-sres-value', value);
                    // Emit event
                    $(sourceElement).trigger(
                        "sres:rangesliderrendered",
                        {
                            columnUuid: columnUuid,
                            subfield: a,
                            identifier: identifier
                        }
                    );
                },
                skin: "round"
            });
        } else if (sliderConfig['slider_mode'] == 'numeric-snap' || sliderConfig['slider_mode'] == 'numeric-free') {
            if (sourceElement.attr('data-sres-value').toString().length == 0) {
                sourceElement.attr('data-sres-value', ENV['rangeslider_config'][columnUuid][a]['values'][0]);
            }
            sourceElement.ionRangeSlider({
                type: 'single',
                min: sliderConfig['values'][0],
                max: sliderConfig['values'][sliderConfig['values'].length - 1],
                step: sliderConfig['step'],
                grid: false,
                onStart: function(data) {
                    renderRangeSliderBadges(data);
                },
                onFinish: function(data) {
                    let columnUuid = data.input.attr('data-sres-columnuuid');
                    let a = data.input.attr('data-sres-subfield');
                    let identifier = data.input.attr('data-sres-identifier');
                    let sliderConfig = ENV['rangeslider_config'][columnUuid][a];
                    if (sliderConfig['slider_mode'] == 'numeric-snap') {
                        // snap to labels
                        // is current value also a label value?
                        if (data.slider.find('.sres-entry-slider-label[data-sres-value="' + data.from + '"]').length) {
                            // already snapped
                        } else {
                            let labels = data.slider.find('.sres-entry-slider-label');
                            for (let l = 0; l < labels.length - 1; l++) {
                                let underValue = parseFloat($(labels[l]).attr('data-sres-value'));
                                let overValue = parseFloat($(labels[l+1]).attr('data-sres-value'));
                                if (data.from > underValue && data.from < overValue) {
                                    let snappedValue = null;
                                    if (overValue - data.from > data.from - underValue) {
                                        //console.log('closer to underValue', overValue, underValue, data.from);
                                        snappedValue = underValue;
                                    } else {
                                        //console.log('closer to overValue', overValue, underValue, data.from);
                                        snappedValue = overValue;
                                    }
                                    updateRangeSlider(columnUuid, a, identifier, snappedValue);
                                }
                            }
                        }
                    }
                    // Emit event
                    $(sourceElement).trigger(
                        "sres:rangesliderrendered",
                        {
                            columnUuid: columnUuid,
                            subfield: a,
                            identifier: identifier
                        }
                    );
                },
                onChange: function(data) {
                    data.input.attr('data-sres-value', data.from);
                },
                skin: "round"
            });
        }
        /*if (typeof ENV['rangeslider_config'][columnUuid][a]['rangesliders'] == 'undefined') {
            ENV['rangeslider_config'][columnUuid][a]['rangesliders'] = {}
        }
        if (typeof identifier != 'undefined') {
            ENV['rangeslider_config'][columnUuid][a]['rangesliders'][identifier] = sourceElement.data('ionRangeSlider');
        }*/
    });
}

$(document).ready(function(){
    if (typeof ENV['rangeslider_config'] !== 'undefined') {
        for (columnUuid of Object.keys(ENV['rangeslider_config'])) {
            for (a of Object.keys(ENV['rangeslider_config'][columnUuid])) {
                renderRangeSlider(columnUuid, a);
            }
        }
    }
});