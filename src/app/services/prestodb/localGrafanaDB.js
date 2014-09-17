define([
],
function () {
  'use strict';

  function LocalGrafanaDB() {
    this.support = false;
    try {
      if ('localStorage' in window && window['localStorage'] !== null) {
        this.support = true;
      }
    } catch (e) {
    }

    this.localStorage = null;
    if (this.support) {
      this.localStorage = window.localStorage;
    }

    this.keys = {'tag': 't', 'title': 'l', 'dashboard': 'd'};
  }

  var l = LocalGrafanaDB.prototype;

  l.getKeys = function() {
    var keys = [];

    if (this.support && this.localStorage !== null) {
      for (var i = 0; i < this.localStorage.length; i++) {
        keys.push(this.localStorage.key(i));
      }
    }
    return keys;
  };

  return LocalGrafanaDB;
});
