define([
  'angular',
  'lodash'
],
function (angular, _) {
  'use strict';

  var module = angular.module('grafana.services');

  module.factory('LocalStorageDatasource', function($q) {

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

    /*
    function objSize(obj) {
      var size = 0, key;
      for (key in obj) {
          if (obj.hasOwnProperty(key)) {
            size++;
          }
      }
      return size;
    }*/

    function objKeys(obj) {
      var keys = [], key;
      for (key in obj) {
          if (obj.hasOwnProperty(key)) {
            keys.push(key);
          }
      }
      return keys;
    }
    
    l.getKeys = function() {
      var keys = [];
    
      if (this.support && this.localStorage !== null) {
        for (var i = 0; i < this.localStorage.length; i++) {
          keys.push(this.localStorage.key(i));
        }
      }
      return keys;
    };
    
    l.getTitles = function() {
      var _this = this;
      if (_this.support && _this.localStorage !== null) {
        if (_this.keys['title'] in _this.localStorage) {
          var d = _this._getItem(_this.keys['title']); 
          return objKeys(d);
        } else {
          return [];
        }
      } else {
        return [];
      }
    };
    
    l.getTags = function() {
      var _this = this;
      if (_this.support && _this.localStorage !== null) {
        if (_this.keys['tag'] in _this.localStorage) {
          var d = _this._getItem(_this.keys['tag']); 
          return objKeys(d);
        } else {
          return [];
        }
      } else {
        return [];
      }
    };

    l._getItem = function(metakey, reset) {
      if (!(metakey in this.localStorage) || reset) {
        this.localStorage.setItem(metakey, JSON.stringify({}));
      }

      var d = JSON.parse(this.localStorage.getItem(metakey));
      return d;
    };

    l._setItem = function(metakey, data) {
      this.localStorage.setItem(metakey, JSON.stringify(data));
    };
 
    l.saveItem = function(metakey, key, value, reset) {

      var d = this._getItem(metakey, reset);

      d[key] = value; 

      this._setItem(metakey, d);     
    };
 
    l.saveListItem = function(metakey, key, value, reset) {

      var d = this._getItem(metakey, reset);

      if (!(key in d) || Object.prototype.toString.call(d[key]) !== '[object Array]') {
        d[key] = [];
      }
      d[key].push(value); 

      this._setItem(metakey, d);     
    };
    
    l._saveDashboard = function(id, title, tags, data) {
      var _this = this;
      var deferred = $q.defer();
      
      setTimeout(function() {
        if (_this.support && _this.localStorage !== null) {
          _this.saveItem(_this.keys['dashboard'], id, data);
          _this.saveListItem(_this.keys['title'], title, id);
      
          var tagList = tags.split(',');
          for (var i in tagList) {
            _this.saveListItem(_this.keys['tag'], tagList[i], id);
          }
      
          deferred.resolve();
        } else {
          deferred.reject();
        }
      }, 0);
      
      return deferred.promise;
    };

    l._deleteDashboard = function(id) {
      var _this = this;
      var deferred = $q.defer();
      
      setTimeout(function() {
        if (_this.support && _this.localStorage !== null) {
          var d = _this._getItem(_this.keys['dashboard']);
          if (id in d) {
            
            // delete dashboard
            delete d[id];
            _this._setItem(_this.keys['dashboard'], d);

            // delete corresponding records in tag
            var tags = _this._getItem(_this.keys['tag']);
            _.each(tags, function(tag, key) {
              var dashboard_ids = tags[key];
              dashboard_ids = _.filter(dashboard_ids, function(d_id) {
                return d_id !== id; 
              });
              tags[key] = dashboard_ids;
            }); 

            _this._setItem(_this.keys['tag'], tags);

            // delete corresponding records in title
            var titles = _this._getItem(_this.keys['title']);
            _.each(titles, function(title, key) {
              var dashboard_ids = titles[key];
              dashboard_ids = _.filter(dashboard_ids, function(d_id) {
                return d_id !== id; 
              });
              titles[key] = dashboard_ids;
            }); 

            _this._setItem(_this.keys['title'], titles);
       
            deferred.resolve(true);
          } else {
            deferred.resolve(false);
          }
        } else {
          deferred.reject();
        }
      }, 0);
      
      return deferred.promise;
    };

    l._searchDashboards = function(query) {
      var _this = this;
      var deferred = $q.defer();
      
      setTimeout(function() {
        if (_this.support && _this.localStorage !== null) {
          var dashboards = [];
          var visited = {};

          if ('tag' in query) {
            var tags = _this._getItem(_this.keys['tag']);

            _.each(tags, function(tag, key) {
              if (query['tag'].test(key)) {
                var dashboard_ids = tags[key];

                _.each(dashboard_ids, function(id) {
                  var target = _this._getDashboardById_Internal(id);
            
                  if (target && 'id' in target && !(target['id'] in visited)) {  
                    dashboards.push(
                      {'id': target['id'],
                      'title': target['title'],
                      'tags': target['tags']});

                    visited[target['id']] = true;
                  }

                });
              }
            });
          }

          if ('title' in query) {
            var titles = _this._getItem(_this.keys['title']);

            _.each(titles, function(title, key) {
              if (query['title'].test(key)) {
                var dashboard_ids = titles[key];

                _.each(dashboard_ids, function(id) {
                  var target = _this._getDashboardById_Internal(id);

                  if (target && 'id' in target && !(target['id'] in visited)) {
                    dashboards.push(
                      {'id': target['id'],
                      'title': target['title'],
                      'tags': target['tags']});

                    visited[target['id']] = true;
                  }

                });
              }
            });
          }
          deferred.resolve(dashboards);
        } else {
          deferred.reject();
        }
      }, 0);
      
      return deferred.promise;
    };

    l._getDashboardById_Internal = function(id) {
      var d = this._getItem(this.keys['dashboard']);
      if (id in d) {
        return d[id][0];
      } else {
        return {};
      }
    };
 
    l._getDashboardById = function(id) {
      var _this = this;
      var deferred = $q.defer();
      
      setTimeout(function() {
        if (_this.support && _this.localStorage !== null) {
          var d = _this._getItem(_this.keys['dashboard']);
          if (id in d) {
            deferred.resolve(d[id]);
          } else {
            deferred.resolve();
          }
        } else {
          deferred.reject();
        }
      }, 0);
      
      return deferred.promise;
        
    };

    l._getDashboard = function(query) {
      if ('id' in query) {
        return this._getDashboardById(query['id']);
      } /*else if ('title' in query) {
      } else if ('tag' in query) {
      }   */
    };
    
    return LocalGrafanaDB;
  });

});

