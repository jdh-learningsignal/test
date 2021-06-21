$(document).ready(function(){
    function sresUpdateScheduleRunDisplay() {
        $('.sres_schedule_run_utc_ts').each(function(){
            let runTs = moment( $(this).attr('data-sres-schedule-run-utc-ts') );
            if (runTs.isValid()) {
                $(this).html(runTs.format('dddd D MMMM YYYY [at] H:mm:ss a') + ' (' + runTs.fromNow() + ')');
            }
        });
    }
    setInterval(sresUpdateScheduleRunDisplay, 60000);
    sresUpdateScheduleRunDisplay();
    $('.sres_schedule_cancel').each(function(){
        $(this).html('<a href="#">Cancel scheduled send</a>.');
    });
    $(document).on('click', '.sres_schedule_cancel', function(){
        if (confirm('Are you sure you wish to cancel the scheduled send?')) {
            $.ajax({
                url: ENV['RUN_PERSONALISED_MESSAGE_ENDPOINT'].replace('__mode__', 'schedule'),
                method: 'DELETE',
                data: {},
                success: function(data){
                    data = JSON.parse(data);
                    //console.log(data);
                    if (data.success) {
                        $.notify({message:"Schedule deleted successfully. Please wait while we refresh the page..."}, {type:'success'});
                    } else {
                        $.notify({message:"An unexpected error occurred while deleting this schedule."}, {type:'danger'});
                    }
                    window.location.reload();
                }
            });
        }
    });
});