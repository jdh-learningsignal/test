$(document).on('click', "button[id^=insert_field_]", function() {
    var textToInsert = $(this).val();
    tinymce.activeEditor.insertContent(textToInsert);
});
$(document).on('click', "#insert_data_from_list", function() {
    show_column_chooser('editor1', '$', null, false);
});

$(document).ready(function(){
    var tinymceBasicToolbar = ['bold italic underline | strikethrough subscript superscript | removeformat | forecolor backcolor | bullist numlist | indent outdent | alignleft aligncenter alignright alignjustify', 'link unlink | image table hr charmap | cut copy paste pastetext | undo redo', 'styleselect fontselect fontsizeselect | code'];
    tinymce.init({
        selector: '#editor1',
        toolbar: tinymceBasicToolbar,
        menubar: false,
        inline: true,
        plugins: 'code textcolor lists link image table hr charmap paste'
    });
});

$(document).on('click', '[data-sres-template-uuid]', function(){
	let newForm = document.createElement('FORM');
	newForm.method = 'POST';
	newForm.action = location.href;
	let csrfToken = document.createElement('INPUT');
	csrfToken.type = 'hidden';
	csrfToken.name = 'csrf_token';
	csrfToken.value = ENV['CSRF_TOKEN'];
	newForm.appendChild(csrfToken);
	let encodedIdentifiers = document.createElement('INPUT');
	encodedIdentifiers.type = 'hidden';
	encodedIdentifiers.name = 'encoded_identifiers';
	encodedIdentifiers.value = $('input[name=encoded_identifiers]').val();
	newForm.appendChild(encodedIdentifiers);
	let templateUuid = document.createElement('INPUT');
	templateUuid.type = 'hidden';
	templateUuid.name = 'template_uuid';
	templateUuid.value = $(this).attr('data-sres-template-uuid');
	newForm.appendChild(templateUuid);
	document.body.appendChild(newForm);
	newForm.submit();
});

