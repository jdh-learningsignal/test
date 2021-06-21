/**
    Accordion sets (multientry)
**/
// Extra save buttons
$(document).on('click', '.sres-addvalue-save-extra', function(){
    let identifier = $(this).attr('data-sres-identifier');
    let columnuuid = $(this).attr('data-sres-columnuuid');
    let additionalClassSelector = $(this).attr('data-sres-additional-class-selector');
    $("button.sres-addvalue-save[data-sres-columnuuid=" + columnuuid + "]" + additionalClassSelector).trigger('click');
});
// Collapses
$(document).on('click', '.sres-accordion-set-btn', function(){
    let $inputContainerSet = $(this).parents('[data-sres-accordion-set-number]');
    let $inputContainer = $inputContainerSet.parents('.sres-input-container');
    let identifier = $inputContainer.attr('data-sres-identifier');
    let columnuuid = $inputContainer.attr('data-sres-columnuuid');
    let accordionSetNumber = $inputContainerSet.attr('data-sres-accordion-set-number');
    console.log(identifier, columnuuid, accordionSetNumber);
    // determine action
    let action = null;
    let scope = null;
    if ($(this).hasClass('sres-accordion-set-collapse')) {
        action = 'hide';
        scope = 'one';
    } else if ($(this).hasClass('sres-accordion-set-collapse-all')) {
        action = 'hide';
        scope = 'all';
    } else if ($(this).hasClass('sres-accordion-set-expand')) {
        action = 'show';
        scope = 'one';
    } else if ($(this).hasClass('sres-accordion-set-expand-all')) {
        action = 'show';
        scope = 'all';
    }
    // iterate
    let lastCollapsibleSetNumber = -1;
    $(".sres-input-container[data-sres-identifier='" + identifier + "'][data-sres-columnuuid=" + columnuuid + "] [data-sres-accordion-set-number]").each(function(){
        let currentSetNumber = $(this).attr('data-sres-accordion-set-number');
        let currentAccordionHeader = $(this).attr('data-sres-accordion-set-header');
        if (scope == 'one' && currentSetNumber == accordionSetNumber && currentAccordionHeader != 'collapsible' && currentAccordionHeader != 'collapsed') {
            $(this).collapse(action);
        } else if (scope == 'all') {
            if (currentAccordionHeader == 'collapsible' || currentAccordionHeader == 'collapsed') {
                lastCollapsibleSetNumber = currentSetNumber;
            } else {
                if (lastCollapsibleSetNumber == currentSetNumber) {
                    $(this).collapse(action);
                }
            }
        }
        if ((currentAccordionHeader == 'collapsible' || currentAccordionHeader == 'collapsed') && (currentSetNumber == accordionSetNumber || scope == 'all')) {
            if (action == 'hide') {
                $(this).find('button.sres-accordion-set-expand').addClass('btn-outline-primary').removeClass('btn-light');
                $(this).find('button.sres-accordion-set-collapse').removeClass('btn-outline-primary').addClass('btn-light');
            } else if (action == 'show') {
                $(this).find('button.sres-accordion-set-collapse').addClass('btn-outline-primary').removeClass('btn-light');
                $(this).find('button.sres-accordion-set-expand').removeClass('btn-outline-primary').addClass('btn-light');
            }
        }
    });
});
