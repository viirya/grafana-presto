define([
  'lodash',
  'moment',
],
function (_, moment) {
  'use strict';

  function PrestoSeries(options) {
    this.seriesList = options.seriesList;
    this.alias = options.alias;
    this.groupByField = options.groupByField;
    this.sinceDate = options.sinceDate;
    this.pseudoNowDate = options.pseudoNowDate;
    this.seriesName = options.alias;
    this.annotation = options.annotation;
    this.intervalSeconds = options.intervalSeconds;
    this.approximate = options.approximate;
  }

  var p = PrestoSeries.prototype;

  p.searchColumns = function(series, field) {
    var i = -1;
    _.each(series.Columns, function(column, index) {
      if (column.name === field) {
        i = index;
      }
    }); 
    return i;
  };

  p.getTimeSeries = function() {
    var output = [];
    var self = this;
    var i;

    if (self.approximate) {
      self.seriesList = self.parseApproximateSeriesList(self.seriesList);
    }

    _.each([self.seriesList], function(series) {
      var timeCol = 0;
      var valueCol = 1;
      var groupByCol = -1;

      if (self.groupByField) {
        groupByCol = self.searchColumns(series, self.groupByField);
      }

      // find value column
      _.each(series.Columns, function(column, index) {
        if (column.name !== 'time' && column.name !== 'sequence_number' && column.name !== self.groupByField) {
          valueCol = index;
        }
      });

      var groups = {};

      if (self.groupByField) {
        groups = _.groupBy(series.Data, function (point) {
          return point[groupByCol];
        });
      }
      else {
        groups[''] = series.Data;
      }

      _.each(groups, function(groupPoints, groupKey) {
        var datapoints = [];
        for (i = 0; i < groupPoints.length; i++) {
          var metricValue = isNaN(groupPoints[i][valueCol]) ? null : groupPoints[i][valueCol];
          var timeValue = groupPoints[i][timeCol] * self.intervalSeconds + moment(self.sinceDate).unix();
          datapoints[i] = [metricValue, timeValue * 1000];
        }

        output.push({ target: self.seriesName + groupKey, datapoints: datapoints });
      });
    });

    return output;
  };

  p.getAnnotations = function () {
    var list = [];
    var self = this;

    _.each([this.seriesList], function (series) {
      var titleCol = null;
      var timeCol = null;
      var tagsCol = null;
      var textCol = null;


      timeCol = self.searchColumns(series, 'time');
      titleCol = self.searchColumns(series, self.annotation.titleColumn);
      tagsCol = self.searchColumns(series, self.annotation.tagsColumn);
      textCol = self.searchColumns(series, self.annotation.textColumn);

      if (!titleCol) {
        titleCol = 1;
      }

      _.each(series.points, function (point) {
        var data = {
          annotation: self.annotation,
          time: point[timeCol] * 1000,
          title: point[titleCol],
          tags: point[tagsCol],
          text: point[textCol]
        };

        if (tagsCol) {
          data.tags = point[tagsCol];
        }

        list.push(data);
      });
    });

    return list;
  };

  p.createNameForSeries = function(seriesName, groupByColValue) {
    var name = this.alias
      .replace('$s', seriesName);

    var segments = seriesName.split('.');
    for (var i = 0; i < segments.length; i++) {
      if (segments[i].length > 0) {
        name = name.replace('$' + i, segments[i]);
      }
    }

    if (this.groupByField) {
      name = name.replace('$g', groupByColValue);
    }

    return name;
  };

  p.parseApproximateSeriesList = function(seriesList) {
    var self = this;

    _.each([seriesList], function (series) {

      var valueCol = 1;

      // find value column
      _.each(series.Columns, function (column, index) {
        if (column.name !== 'time' && column.name !== 'sequence_number' && column.name !== self.groupByField) {
          valueCol = index;
        }
      });

      var parsedPoints = _.map(series.Data, function(point) {
        if (point[valueCol]) {
          var res = point[valueCol].match(/(.*)\+\/-/);
          if (res) {
            if (res[1].match(/\./)) {  
              point[valueCol] = parseFloat(res[1]);  
            } else {
              point[valueCol] = parseInt(res[1]);
            }
          } else {
            point[valueCol] = 0;
          }
        } else {
          point[valueCol] = 0;
        }
        return point;
      });

      series.Data = parsedPoints;

    });

    return seriesList;
 
  };

  return PrestoSeries;
});
