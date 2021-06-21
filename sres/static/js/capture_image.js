function capture_show_image(inputSelector, canvasSelector, maxDimension){
	var input = $(inputSelector)[0];
	var file = input.files[0];
	if (typeof maxDimension === 'undefined') {
		maxDimension = 200;
	}
	capture_draw_on_canvas(file, canvasSelector, parseInt(maxDimension), parseInt(maxDimension));
}

function capture_draw_on_canvas(file, canvasSelector, canvas_max_width, canvas_max_height) {
  var reader = new FileReader();
  reader.onload = function (e) {
	var dataURL = e.target.result,
		c = $(canvasSelector)[0], 
		ctx = c.getContext('2d'),
		img = new Image();
	img.onload = function() {
		var MAX_WIDTH = canvas_max_width;
		var MAX_HEIGHT = canvas_max_height;
		var width = img.width;
		var height = img.height;
		if (width > height) {
		  if (width > MAX_WIDTH) {
			height *= MAX_WIDTH / width;
			width = MAX_WIDTH;
		  }
		} else {
		  if (height > MAX_HEIGHT) {
			width *= MAX_HEIGHT / height;
			height = MAX_HEIGHT;
		  }
		}
		c.width = width;
		c.height = height;
		var ctx = c.getContext("2d");
		//ctx.drawImage(img, 0, 0, width, height);
		drawImageIOSFix(ctx, img, 0, 0, img.width, img.height, 0, 0, width, height);
	};
	img.src = dataURL;
  };
  reader.readAsDataURL(file);
}		

/* http://stackoverflow.com/questions/11929099/html5-canvas-drawimage-ratio-bug-ios */
/**
 * Detecting vertical squash in loaded image.
 * Fixes a bug which squash image vertically while drawing into canvas for some images.
 * This is a bug in iOS6 devices. This function from https://github.com/stomita/ios-imagefile-megapixel
 * 
 */
function detectVerticalSquash(img) {
    var iw = img.naturalWidth, ih = img.naturalHeight;
    var canvas = document.createElement('canvas');
    canvas.width = 1;
    canvas.height = ih;
    var ctx = canvas.getContext('2d');
    ctx.drawImage(img, 0, 0);
    var data = ctx.getImageData(0, 0, 1, ih).data;
    // search image edge pixel position in case it is squashed vertically.
    var sy = 0;
    var ey = ih;
    var py = ih;
    while (py > sy) {
        var alpha = data[(py - 1) * 4 + 3];
        if (alpha === 0) {
            ey = py;
        } else {
            sy = py;
        }
        py = (ey + sy) >> 1;
    }
    var ratio = (py / ih);
    return (ratio===0)?1:ratio;
}
/**
 * A replacement for context.drawImage
 * (args are for source and destination).
 */
function drawImageIOSFix(ctx, img, sx, sy, sw, sh, dx, dy, dw, dh) {
    var vertSquashRatio = detectVerticalSquash(img);
 // Works only if whole image is displayed:
 // ctx.drawImage(img, sx, sy, sw, sh, dx, dy, dw, dh / vertSquashRatio);
 // The following works correct also when only a part of the image is displayed:
    ctx.drawImage(img, sx * vertSquashRatio, sy * vertSquashRatio, 
                       sw * vertSquashRatio, sh * vertSquashRatio, 
                       dx, dy, dw, dh );
}
