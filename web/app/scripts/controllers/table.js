'use strict';

angular.module('basic').controller('TableCtrl', ['$scope', '$http', 'GLOBAL', '$uibModal', '$ngConfirm', function ($scope, $http, GLOBAL, $uibModal, $ngConfirm) {
  // Tool funcs
  // Check weight val of queried fields
  $scope.queryWeight = function(field, query_fields) {
    for (let qfield of query_fields) {
      if (field === qfield.name) {
        return qfield.weight;
      }
    }
    return null;
  };

  // Table operations
  // 1, addTable
  // 2, selectTable
  // Add table
  $scope.addTable = function() {
    $http.get(GLOBAL.host+"/schema/list").then(function(data) {
      $scope.schemas = data.data.schemas;
      let modalInstance = $uibModal.open({
        animation: true,
        templateUrl: 'addTable.html',
        backdrop: 'static',
        scope: $scope,
        size: 'lg',
        controller: ['$scope', '$http', '$ngConfirm', function($scope, $http, $ngConfirm) {
          $scope.name = 'top';
          $scope.newtable = {
            name:"",
            schema:"",
            hbase:{},
            solr:{}
          };
          $scope._ok = function(){
            //console.log($scope.newtable);
            $http.post(GLOBAL.host+"/table/create", $scope.newtable).then(function() {
              modalInstance.close();
            });
          };
          $scope.ok = function(){
            $ngConfirm({
              title: 'Confirmation',
              content: 'Create table now?',
              scope: $scope,
              closeIcon: true,
              buttons: {
                Yes:{
                  text: 'Yes',
                  action: function(scope){
                    scope._ok();
                    scope.initial();
                  }
                },
                No:{
                  text: "No"
                }
              }
            });
          };
          $scope.cancel = function(){
            modalInstance.close();
          };
        }]
      });
    });
  };
  // Edit table
  $scope.editTable = function() {
    let modalInstance = $uibModal.open({
      animation: true,
      templateUrl: 'editTable.html',
      backdrop: 'static',
      scope: $scope,
      size: 'lg',
      controller: ['$scope', '$http', '$ngConfirm', function($scope, $http, $ngConfirm) {
        $scope.ok = function() {
          $ngConfirm({
            title: "Confirmation",
            content: "",
          });
        };
        $scope.cancel = function() {
          modalInstance.close();
        };
      }]
    });
  };
  // Delete table
  $scope.deleteTable = function() {
    $ngConfirm({
      title: "Confirmation",
      content: "Want to delete the table selected? All data related will be lost.",
      closeIcon: true,
      buttons: {
        Yes: {
          text: "Yes",
          action: function() {
            $http.post(GLOBAL.host+"", {name:$scope.page.table.name}).then(function() {
              $scope.initial();
            });
          }
        },
        No: {
          text: "No"
        }
      }
    });
  };
  // Select table
  $scope.selectTable = function(table, index){
    $http.get(GLOBAL.host+"/schema/get", {params:{type:"table",name:table}}).then(function(data) {
      $scope.page.table.name = table;
      $scope.page.table.schema = data.data.schema.name;
      $scope.page.table.fields = data.data.schema.fields;
      $scope.page.table.query_fields = data.data.schema.query_fields;
      $scope.page.tablesActive = [];
      for(let i = 0 ; i < $scope.tables.length; i++){
        if(i === index){
          $scope.page.tablesActive.push(true);
        }else {
          $scope.page.tablesActive.push(false);
        }
      }
    });
  };
  // Refresh tables
  $scope.initial = function() {
    $scope.page = {
      table: {},
      tablesActive: []
    };
    $http.get(GLOBAL.host+"/table/list").then(function(data) {
      $scope.tables = data.data.tables;
      $scope.selectTable($scope.tables[0], 0);
    });
  };

  // initial steps
  $scope.initial();
}]);
