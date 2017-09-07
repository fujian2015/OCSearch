'use strict';

/**
 * Search Result Controller
 */
angular.module('basic').controller('ResultCtrl', ['$scope', 'searchService', '$stateParams', 'hotkeys', '$http', 'GLOBAL', '$window', function ($scope, searchService, $stateParams, hotkeys, $http, GLOBAL, $window) {
  /**
   * Initialize params
   */
  $scope.content = $stateParams.content;
  $scope.schemas = $stateParams.schemas;
  $scope.condition = "";
  $scope.host = GLOBAL.host;
  $scope.page = {
    schema : {},
    tables: [],
    fields: [],
    actives: [],
    rows: 10,
    rowOptions:[10,20,30,50],
    pagination:{
      current : 1,
      maxsize : 10
    },
  };

  /**
   * Page Methods;
   */
  $scope.search = function(){
    $scope.showAdvance = false;
    searchService.search($scope.content, function(schemas){
      $scope.schemas = schemas;
      if(schemas && schemas.length > 0) {
        if (Object.keys($scope.page.schema).length !== 0) {
          for (let i=0; i<schemas.length; ++i) {
            if (schemas[i].name === $scope.page.schema.name) {
              $scope.chooseSchema(schemas[i], i);
              break;
            }
          }
        } else {
          $scope.chooseSchema(schemas[0], 0);
        }
      }
    });
  };

  $scope._choose = function(){
    let tables = [];
    for(let i = 0; i < $scope.page.tables.length; i++){
      if($scope.page.actives[i]){
        tables.push($scope.page.tables[i]);
      }
    }
    if (!$scope.content) { $scope.content=""; }
    let query_condition = {
      "query": $scope.content,
      "condition": $scope.condition,
      "start": ($scope.page.pagination.current-1) * $scope.page.rows,
      "rows": $scope.page.rows,
      "sort": "",
      "tables": tables,
      "return_fields": $scope.page.fields
    };
    $http.post(GLOBAL.host + '/query/search', query_condition).then(function(data){
      $scope.data = data.data.data;
      if($scope.data) {
        $scope.page.pagination.total = $scope.data.total;
      }
    });
  };

  $scope.choose = function(table){
    if($scope.page.tables) {
      for (let i = 0; i < $scope.page.tables.length; i++) {
        if ($scope.page.tables[i] === table) {
          $scope.page.actives[i] = !$scope.page.actives[i];
          break;
        }
      }
    }
    $scope._choose();
  };

  $scope.chooseSchema = function(item, index){
    $scope.page.pagination.current = 1;
    $scope.page.schema = item;
    $scope.page.chooseIndex = index;
    $scope.page.tables = item.tables;
    $scope.page.actives = [];
    if(item.tables && item.tables.length > 0){
      for(let i = 0; i < item.tables.length; i++){
        $scope.page.actives[i] = true;
      }
    }
    $scope.page.fields = ["id", "_table_"];
    for(let i = 0; i < item.fields.length; i++){
      $scope.page.fields.push(item.fields[i].name);
    }
    $scope._choose();
  };

  $scope.pageChanged = function(){
    $scope._choose();
  };

  $scope.toggleSideBar = function(){
    $scope.$broadcast('openSidebar');
  };

  $scope.initShow = function(item, idx) {
    $scope.showCtrl[idx] = [];
    for (let f of $scope.page.schema.fields) {
      if (angular.isDefined($scope.schema_display[$scope.page.schema.name]) && $scope.schema_display[$scope.page.schema.name][f.name]) {
        let temp = {};
        if (angular.isDefined(item[f.name])) {
          temp[f.name] = item[f.name];
        } else {
          temp[f.name] = null;
        }
        $scope.showCtrl[idx].push(temp);
      }
    }
  };
  $scope.getColor = function(idx) {
    let colors = ["result-odd", "result-even"];
    return colors[parseInt(idx/4) % 2];
  };
  $scope.valFilter = function(item, len) {
    let key = Object.keys(item)[0];
    let val = item[key];
    if (angular.isString(val)) {
      if (val.length >= len) {
        val = val.substring(0, len) + "...";
      }
      let temp = {};
      temp[key] = val;
      return temp;
    } else if (angular.isArray(val)) {
      // Todo
      let lst = [];
      for (let a of val) {
        if (angular.isString(a)) {
          if (a.length > len/val.length) {
            lst.push(a.substring(0, parseInt(len/val.length)) + "...");
          }
        }
      }
      let temp = {};
      temp[key] = lst.join(",");
      return temp;
    } else {
      return item;
    }
  };
  /**
   * Global init functions
   */
  $scope.showCtrl = [];
  $scope.showAdvance = false;
  $http.get("/schema/config").then(function(data) {
    $scope.schema_display = data.data;
    if($scope.schemas && $scope.schemas.length > 0) {
      $scope.chooseSchema($scope.schemas[0], 0);
    }
    let focusElem = $window.document.getElementById('mainSearchInput');
    if (focusElem) {
      focusElem.focus();
    }
  });

  hotkeys.bindTo($scope)
    .add({
      combo: 'enter',
      allowIn: ['INPUT', 'SELECT', 'TEXTAREA'],
      callback: $scope.search
    });
}])
.directive('searchFocus', function($timeout) {
  return function(scope, element) {
    scope.$watch('showAdvance', function(newVal) {
      $timeout(function () {
        if (newVal) {
          element[0].focus();
        }
      });
    }, true);
  };
});
