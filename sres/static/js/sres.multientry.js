/**
    Gathers up the values from multientry subfields
**/
function collectMultientryData(sourceElements) {
    let tmpData = [];
    sourceElements.each(function() {
        let sourceElement = $(this);
        // check if disabled
        if (false /*sourceElement.prop('disabled')*/) {
            // works for slider, regex, regex-long, audio-recording, sketch-small
            tmpData.push('');
        } else {
            // proceed with grabbing data
            switch (sourceElement.attr('data-sres-field')) {
                case 'select':
                case 'dropdown':
                    let tmpArr = [];
                    switch (sourceElement.attr('data-sres-field')) {
                        case 'select':
                            sourceElement.find("button[class~='btn-primary']").each(function(){
                                if (false /*$(this).prop('disabled')*/) {
                                    // do nothing
                                } else {
                                    tmpArr.push($(this).attr('data-sres-value'));
                                }
                            });
                            break;
                        case 'dropdown':
                            sourceElement.find("select option:selected").each(function() {
                                if (false /*$(this).prop('disabled')*/) {
                                    // do nothing
                                } else {
                                    tmpArr.push($(this).attr('data-sres-value'));
                                }
                            });
                            break;
                    }
                    if (sourceElement.attr('data-sres-selectmode') == 'single' && (tmpArr.length == 1 || tmpArr.length == 0)) {
                        tmpArr = tmpArr.join();
                    }
                    tmpData.push(tmpArr);
                    break;
                case 'regex':
                case 'regex-long':
                    tmpData.push(sourceElement.val());
                    break;
                case 'html-simple':
                    if ( tinymce.get( sourceElement.attr('id') ) ) {
                        tmpData.push(tinymce.get( sourceElement.attr('id') ).getContent());
                    } else {
                        tmpData.push('');
                    }
                    break;
                case 'slider':
                    tmpData.push(sourceElement.attr('data-sres-value'));
                    break;
                case 'audio-recording':
                    let tmpAudioArr = [];
                    sourceElement.find('.sres-audio-recording-single-recording audio[data-sres-audio-filename]').each(function(){
                        tmpAudioArr.push($(this).attr('data-sres-audio-filename'));
                    });
                    tmpData.push(tmpAudioArr);
                    break;
                case 'sketch-small':
                    tmpData.push(sourceElement.val());
                    break;
                case 'timestamp':
                    var d = new Date();
                    tmpData.push(d.getFullYear() + '-' + (d.getMonth() + 1 < 10 ? '0' : '') + (d.getMonth() + 1) + '-' + (d.getDate() < 10 ? '0' : '') + d.getDate() + ' ' + (d.getHours() < 10 ? '0' : '') + d.getHours() + (d.getMinutes() < 10 ? ':0' : ':') + d.getMinutes() + (d.getSeconds() < 10 ? ':0' : ':') + d.getSeconds());
                    break;
                case 'authuser':
                    tmpData.push(sourceElement.find("input:hidden").val());
                    break;
                case 'geolocation':
                    tmpData.push(sourceElement.find("input:hidden").val());
                    break;
                case 'label-only':
                    tmpData.push('');
                    break;
            }
        }
    });
    return tmpData;
}

/**
    Calculates the in-column aggregation/calculation for a multientry subfield
**/
function performMultientryAggregation(inputValues, blankTreatment, calculationMethod, roundingMethod, returnArray, multiplier, valuesMultipliers) {
    let values = [];
    let finalValue = null;
    // collect
    inputValues.forEach(function(currentValue) {
        if (typeof currentValue == 'object' && currentValue.length) {
            tmp = performMultientryAggregation(currentValue, blankTreatment, null, null, true);
            values = values.concat(tmp);
        } else {
            if (isNaN(currentValue) || currentValue == '' || currentValue.length == 0) {
                if (blankTreatment == 'ignore') {
                    currentValue = null;
                } else if (blankTreatment == 'convertzero') {
                    currentValue = 0;
                }
            } else {
                currentValue = parseFloat(currentValue);
            }
            if (currentValue !== null) {
                values.push(currentValue);
            }
        }
    });
    // Perform multipliers if needed
    if (typeof valuesMultipliers !== 'undefined') {
        let multipliedValues = [];
        for (let i = 0; i < values.length; i++) {
            let valueMultiplier = valuesMultipliers[i];
            let value = values[i];
            if (typeof valueMultiplier !== 'undefined') {
                multipliedValues.push(value * parseFloat(valueMultiplier));
            } else {
                multipliedValues.push(value);
            }
        }
        values = multipliedValues;
    }
    // Continue
    if (returnArray) {
        // return straight away, no calculation
        finalValue = values;
    } else {
        // calculate
        let valuesSum = 0;
        values.forEach(function(value) {
            valuesSum += value;
        });
        if (calculationMethod == 'sum') {
            finalValue = valuesSum;
        } else if (calculationMethod == 'average') {
            finalValue = valuesSum / values.length;
        }
        // multiplier if required
        if (typeof multiplier === 'undefined') {
            multiplier = 1;
        }
        finalValue = finalValue * parseFloat(multiplier);
        // rounding if needed
        if (roundingMethod == 'no') {
            // do nothing
        } else {
            let dp = parseInt(roundingMethod);
            finalValue = finalValue.toFixed(dp);
        }
    }
    // return
    return finalValue;
}

/**
    Triggers multientry aggregation/calculation
**/
function triggerMultientryAggregation(type, args) {
    let id = args.columnUuid + '_' + args.subfield + '_' + args.identifier;
    switch (type) {
        case 'regex':
        case 'dropdown':
            $(document).on('input changed.bs.select change', '#' + id, function(){
                args.sourceElement = $(this);
                args.value = $(this).val();
                args.identifier = $(this).attr('data-sres-identifier') || $(this).parents('[data-sres-field]').attr('data-sres-identifier') || args.identifier;
                $(document).trigger("sres:addvaluesubfieldchanged", args);
            });
            $('#' + id).trigger('input', {doNotProcessDirty:true});
            break;
        case 'select':
            $(document).on('click input', '[data-sres-target-id="' + id + '"] button', function(){
                args.sourceElement = $(this);
                args.value = $(this).attr('data-sres-value');
                args.identifier = $(this).parents('[data-sres-field]').attr('data-sres-identifier') || args.identifier;
                $(document).trigger("sres:addvaluesubfieldchanged", args);
            });
            $('[data-sres-target-id="' + id + '"] button').trigger('input', {doNotProcessDirty:true});
            break;
        case 'slider':
            $(document).on('sres:rangesliderrendered', '#' + id, function(){
                args.sourceElement = $('#' + id);
                args.value = $(this).attr('data-sres-value');
                args.identifier = $(this).attr('data-sres-identifier') || args.identifier;
                $(document).trigger("sres:addvaluesubfieldchanged", args);
            });
            $('#' + id).trigger('sres:rangesliderrendered');
            break;
    }
}

