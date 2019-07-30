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
  } 
};