import { record } from "./vmsg.js";

var sresAudioRecordingContainerHTML = '<div class="sres-audio-recording-single-recording mb-2">' + 
    '<div class="d-none badge badge-danger ml-2 sres-audio-recording-status-not-uploaded"><span class="fa fa-exclamation-triangle"></span> Not yet uploaded</div>' + 
    '<button type="button" class="btn btn-outline-primary ml-2 sres-audio-recording-upload-recording sres-ignore-dirty"><span class="fa fa-file-upload"></span> Upload audio clip</button>' + 
    '<div class="d-none badge badge-info ml-2 sres-audio-recording-status-uploading"><span class="fa fa-circle-notch spinning"></span> Uploading...</div>' + 
    '<div class="d-none badge badge-success ml-2 sres-audio-recording-status-uploaded"><span class="fa fa-check-circle"></span> Clip uploaded</div>' + 
    '<div class="d-none badge badge-info ml-2 sres-audio-recording-status-unsaved"><span class="fa fa-info-circle"></span> Remember to save</div>' + 
    '<div class="d-none badge badge-success ml-2 sres-audio-recording-status-saved"><span class="fa fa-check-circle"></span> Clip uploaded and saved</div>' + 
    '<button type="button" class="btn btn-outline-danger ml-2 sres-audio-recording-delete-recording sres-ignore-dirty" aria-label="Delete"><span class="fa fa-trash"></span></button>' + 
    '<div class="d-none badge badge-warning ml-2 sres-audio-recording-restored-from-backup">Restored from device backup</div>' + 
    '</div>';

// https://stackoverflow.com/a/2117523
function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

function showSingleRecording(url, recordingsContainer, id, restoredFromBackup) {
    
    if (!id) {
        let id = 'sres-audio-' + uuidv4();
    }
    let audioControl = document.createElement('audio');
    audioControl.controls = true;
    audioControl.src = url;
    audioControl.id = id;
    audioControl.style = "height: 2.5rem;";
    $(audioControl).addClass('align-middle');
    
    let recordingElement = $(sresAudioRecordingContainerHTML);
    recordingElement.prepend(audioControl);
    recordingsContainer.append(recordingElement);
    
    if (restoredFromBackup) {
        recordingElement.find('.sres-audio-recording-restored-from-backup').removeClass('d-none');
    }
    
    if (recordingsContainer.attr('readonly')) {
        console.log('readonly!');
        recordingElement.find('.sres-audio-recording-status-uploaded').addClass('d-none');
        recordingElement.find('.sres-audio-recording-delete-recording').addClass('d-none');
        recordingElement.find('.sres-audio-recording-upload-recording').addClass('d-none');
    }
    
    return recordingElement;
    
}

$(document).on('click', 'button.sres-audio-recording-record', function(event, eventParams){
    
    let recordingsContainer = $(this).siblings('.sres-audio-recording-recordings-container'); //.get(0);
    
    //record({wasmURL: "/static/js/vmsg.wasm"}).then(blob => {
    record().then(blob => {
        console.log("Recorded MP3", blob);
        
        let id = 'sres-audio-' + uuidv4();
        
        let url = URL.createObjectURL(blob);
        let recordingElement = showSingleRecording(url, recordingsContainer, id);
        recordingElement.find('.sres-audio-recording-upload-recording').addClass('flash animated delay-1s'); // attract attention
        recordingElement.find('.sres-audio-recording-status-not-uploaded').removeClass('d-none');
        
        // Save to local storage
        localforage.setItem(id, { 
                                    blob: blob,
                                    containerId: recordingsContainer.attr('id'),
                                    identifier: recordingsContainer.attr('data-sres-identifier'),
                                    columnUuid: recordingsContainer.attr('data-sres-columnuuid')
                                }
                            ).then(function(value){
            console.log('sres-audio saved', id, value);
        });
        
    });
});

/**
    Upload recording to server
**/
$(document).on('click', 'button.sres-audio-recording-upload-recording', function() {
    let audioControl = $(this).siblings("audio[id^='sres-audio-']");
    
    audioControl.siblings('.sres-audio-recording-status-uploading').removeClass('d-none');
    
    let xhr = new XMLHttpRequest()
    xhr.open('GET', audioControl.attr('src'), true);
    xhr.responseType = 'blob';
    xhr.onload = function(e) {
        if (this.status == 200) {
            // Get the blob
            let blob = this.response;
            // Turn blob into file
            let audioFile = new File(
                [blob],
                audioControl.attr('id') + '.mp3',
                {type: 'audio/mpeg'}
            );
            // Upload
            let identifier = audioControl.parents('.sres-audio-recording-recordings-container').attr('data-sres-identifier');
            let columnUuid = audioControl.parents('.sres-input-container[data-sres-columnuuid]').attr('data-sres-columnuuid');
            let formData = new FormData();
            formData.append('d', audioFile);
            formData.append('i', identifier);
            formData.append('c', columnUuid);
            formData.append('t', 'audio');
            $.ajax({
                url: ENV['SEND_RICH_DATA_ENDPOINT'],
                method: 'POST',
                data: formData,
                processData: false,
                contentType: false,
                success: function(data){
                    data = JSON.parse(data);
                    console.log(data);
                    audioControl.attr('data-sres-audio-filename', data.data);
                    audioControl.siblings('.sres-audio-recording-status-uploaded').removeClass('d-none');
                    audioControl.siblings('.sres-audio-recording-status-unsaved').removeClass('d-none');
                    audioControl.siblings('.sres-audio-recording-upload-recording').addClass('d-none');
                    audioControl.siblings('.sres-audio-recording-status-not-uploaded').addClass('d-none');
                    // Remove offline backup
                    localforage.removeItem(audioControl.attr('id'));
                }
            }).done(function(){
                audioControl.siblings('.sres-audio-recording-status-uploading').addClass('d-none');
            });
        }
    }
    xhr.send();
});

/**
    Recording deletion
**/
$(document).on('click', 'button.sres-audio-recording-delete-all', function(event, eventParams){
    console.log(event);
    $(this).siblings('.sres-audio-recording-recordings-container').find('.sres-audio-recording-single-recording button.sres-audio-recording-delete-recording').trigger('click');
});
$(document).on('click', 'button.sres-audio-recording-delete-recording', function() {
    let audioControl = $(this).siblings("audio[id^='sres-audio-']");
    console.log('trying to delete', audioControl);
    localforage.removeItem(audioControl.attr('id'));
    URL.revokeObjectURL(audioControl.attr('src'));
    $(this).parents('.sres-audio-recording-single-recording').remove();
});

/**
    Display any backed up recordings
**/
$(document).on('sres:audiorecordingshowbackups', function(){
    // get the container ids for all available containers
    let existingAudioContainerIds = [];
    $('.sres-audio-recording-recordings-container').each(function(){
        existingAudioContainerIds.push($(this).attr('id'));
    });
    // iterate local storage
    localforage.iterate(function(value, key, iterationNumber){
        if (key.startsWith('sres-audio-')) {
            if (existingAudioContainerIds.indexOf(value.containerId > -1)) {
                let recordingsContainer = $('#' + value.containerId);
                let identifier = recordingsContainer.attr('data-sres-identifier');
                let columnUuid = recordingsContainer.attr('data-sres-columnuuid');
                if (identifier == value.identifier && columnUuid == value.columnUuid) {
                    if ($('#' + key).length) {
                        // already exists, don't add again
                    } else {
                        let url = URL.createObjectURL(value.blob);
                        showSingleRecording(url, $('#' + value.containerId), key, true);
                    }
                } else {
                    console.log('not matching', value, identifier, columnUuid);
                }
            }
        }
    });
});

/**
    Display any properly-saved recordings
**/
$(document).on('sres:audiorecordingsloadaudio', function(event, eventParams){
    // load the audio controls and src
    $('.sres-audio-recording-recordings-container[data-sres-saved-recordings]').each(function(){
        let recordingContainer = $(this);
        let savedRecordings = JSON.parse($(this).attr('data-sres-saved-recordings'));
        if (savedRecordings.length) {
            // First clear
            recordingContainer.html('');
            // Then add
            savedRecordings.forEach(function(savedRecordingFilename){
                let recordingElement = showSingleRecording(ENV['GET_FILE_ENDPOINT'].replace('__filename__', savedRecordingFilename), recordingContainer);
                //recordingElement.find('.sres-audio-recording-status-uploaded').removeClass('d-none');
                recordingElement.find('.sres-audio-recording-status-saved').removeClass('d-none');
                recordingElement.find('.sres-audio-recording-upload-recording').addClass('d-none');
                recordingElement.find('audio').attr('data-sres-audio-filename', savedRecordingFilename);
            });
        }
    });
});
$(document).ready(function(){
    $(document).trigger('sres:audiorecordingsloadaudio');
});