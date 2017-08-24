'use strict';

angular.module("helpapp", [])
  .constant('GLOBAL', {host: './ocsearch-service'})
  .controller("expressionCtrl", function($scope, $http, GLOBAL) {
    $scope.helpTitle = "How to build expression";
    $http.get(GLOBAL.host+"/expression/list").then(function(response) {
      $scope.expressions = response.data.expressions;
      $scope.expobjs = [];
      for (let obj in $scope.expressions) {
        $scope.expobjs.push(obj);
      }
      console.log($scope.expressions);
    });
  });