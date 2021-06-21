var sketchables = {};
$(document).ready(function () {
    prepareSketchables();
});

function loadImageToCanvas(src, htmlCanvas, containerWidth) {

    let sketch = new Image;
    let canvasEditingContext = htmlCanvas.getContext("2d");

    sketch.onload = function () {

        htmlCanvas.height = 100;
        let sketchWidth = sketch.width;
        let sketchHeight = sketch.height;

        if (sketchWidth > containerWidth) {
            let percentScale = containerWidth / sketch.width;
            sketchWidth = sketch.width * percentScale;
            sketchHeight = sketch.height * percentScale;
        }

        canvasEditingContext.drawImage(sketch, 0, 0, sketchWidth, sketchHeight);
    }

    sketch.src = src;

}

function clearSketchCanvas(canvas) {
    var ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, canvas.width, canvas.height);
}

function loadDataToCanvas(id) {
    var currentCanvas = $("#" + id).get(0);
    var existingData = $(currentCanvas).parents(".sres-sketch-container").siblings("input:hidden").val();
    var containerWidth = $(currentCanvas).parents(".sres-sketch-container").get(0).clientWidth;
    if (existingData != "") {
        loadImageToCanvas(existingData, currentCanvas, containerWidth);
    } else {
        clearSketchCanvas(currentCanvas);
    }
}

function prepareSketchables() {
    var sketchCanvases = document.getElementsByClassName('sres-sketch-area');
    for (var i = 0; i < sketchCanvases.length; i++) {
        var w = $(sketchCanvases[i]).parents(".sres-sketch-container").get(0).clientWidth;
        var h = $(sketchCanvases[i]).parents(".sres-sketch-container").get(0).clientHeight;
        var id = $(sketchCanvases[i]).attr("id");
        //sketchCanvases[i].style.width = w;
        sketchCanvases[i].width = w;
        if ($(sketchCanvases[i]).parents(".sres-sketch-container").siblings("input:hidden").prop("readonly")) {
            // Skip making it sketchable
        } else {
            sketchables[id] = new Sketchable(sketchCanvases[i], {
                interactive: true,
                graphics: {
                    firstPointSize: 3,
                    lineWidth: 3,
                    strokeStyle: '#000',
                    fillStyle: '#000',
                    lineCap: 'round',
                    lineJoin: 'round',
                    miterLimit: 10
                },
                events: {
                    mouseup: function (elem, data, evt) {
                        $(elem).parents(".sres-sketch-container").siblings("input:hidden").val($(elem).get(0).toDataURL());
                    }
                }
            });
        }
        // load existing data if exists
        loadDataToCanvas(id);
        /*var existingData = $(sketchCanvases[i]).parents(".sres-sketch-container").siblings("input:hidden").val();
        var currentCanvas = $("#" + id).get(0);
        if (existingData != "") {
            loadImageToCanvas(existingData, currentCanvas, w);
        } else {
            clearSketchCanvas(currentCanvas);
        }*/
    }
};
$(document).on("click", ".sres-sketch-clear", function () {
    $canvas = $(this).siblings('.sres-sketch-container').find('.sres-sketch-area');
    clearSketchCanvas($canvas.get(0));
    $(this).siblings("input:hidden").val("");
});
