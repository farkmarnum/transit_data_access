module.exports = {
  formatBytes: function (bytes, decimals) {
    if (bytes === 0) {
      return '0 Bytes';
    }
    var k = 1024;
    var dm = decimals <= 0 ? 0 : decimals || 2;
    var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
  },
  sleep: function (ms) {
  	return new Promise(resolve => setTimeout(resolve, ms))
  },
  formatSecondsFull: function(timeInSeconds) {
    var pad = function(num, size) { return ('00' + num).slice(size * -1); },
    hours = Math.floor(timeInSeconds / 60 / 60),
    minutes = Math.floor(timeInSeconds / 60) % 60,
    seconds = Math.floor(timeInSeconds - minutes * 60 - hours * 60 * 60)
    if (seconds + minutes * 60 + hours * 3600 !== timeInSeconds) {
      console.error('formatSeconds assertion error: ', timeInSeconds, seconds + minutes * 60 + hours * 3600 )
    }

    if (hours > 0) {
      return pad(hours, 1) + ':' + pad(minutes, 2) + ':' + pad(seconds, 2)
    } else {
      return pad(minutes, 1) + ':' + pad(seconds, 2)
    }
  }
}