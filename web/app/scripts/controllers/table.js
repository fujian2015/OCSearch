'use strict';

angular.module('basic').controller('TableCtrl', ['$scope', '$http', 'GLOBAL', '$uibModal', '$ngConfirm', '$translate', function ($scope, $http, GLOBAL, $uibModal, $ngConfirm, $translate) {
  let yes_text = $translate.instant('YES');
  let no_text = $translate.instant('NO');
  let confirmation_text = $translate.instant('CONFIRMATION');
  let create_table_text = $translate.instant('CONFIRM_ADD_TABLE');
  let edit_table_text = $translate.instant('CONFIRM_EDIT_TABLE');
  let delete_table_text = $translate.instant('CONFIRM_DELETE_TABLE');
  // Tool funcs
  // Check weight val of queried fields
  $scope.queryWeight = function(schema, index) {
    let field = schema.fields[index].name;
    for (let qfield of schema.query_fields) {
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
            hbase:{
              region_num: 0,
              region_split: []
            },
            solr:{
              shards: 0,
              replicas: 0
            }
          };
          $scope.checkAddTable = function() {
            if ($scope.newtable.name === "" || $scope.newtable.schema === "") {
              $scope.modalmsg = $translate.instant('MODALMSG_FILL_IN_ALL');
              return false;
            } else {
              $scope.modalmsg = "";
              return true;
            }
          };
          $scope._ok = function(){
            if ($scope.newtable.hbase.region_split === "" || $scope.newtable.hbase.region_split === null) { $scope.newtable.hbase.region_split = []; }
            //console.log($scope.newtable);
            $http.post(GLOBAL.host+"/table/create", $scope.newtable).then(function() {
              //console.log(data);
              $scope.initial();
              modalInstance.close();
            });
          };
          $scope.ok = function(){
            $ngConfirm({
              title: confirmation_text,
              content: create_table_text,
              scope: $scope,
              closeIcon: true,
              buttons: {
                Yes:{
                  text: yes_text,
                  action: function(scope){
                    scope._ok();
                  }
                },
                No:{
                  text: no_text
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
        // Change request
        $scope.request_list = {add_field:[],delete_field:[],update_field:[]};
        // Copy page.table.schema
        $scope.curschema = angular.copy($scope.page.table.schema);
        // Temporary field
        $scope.curfield = {name:"", store_type:"", indexed:false};
        // field store type
        $scope.field_type = [ 
          "INT", "LONG", "FLOAT", "DOUBLE", "STRING", "BOOLEAN", "FILE", "ATTACHMENT"
        ];
        // field index type
        $scope.field_index_type = [
          "int", "int_d", "ints", "tint", "tint_d", "tints",
          "double", "double_d", "doubles", "tdouble", "tdouble_d", "tdoubles",
          "float", "float_d", "floats", "tfloat", "tfloat_d", "tfloats",
          "long", "long_d", "longs", "tlong", "tlong_d", "tlongs",
          "string", "string_d", "strings", "lowercase",
          "tdate", "tdate_d", "tdates", "text_en", "text_general", "text_ik"
        ];
        // Functions of filter
        $scope.typeFilter = {
          CONTENT: function(item) { return /^text/.test(item); },
          INT: function(item) { return /int/.test(item); },
          FLOAT: function(item) { return /float/.test(item); },
          DOUBLE: function(item) { return /double/.test(item); },
          LONG: function(item) { return /long/.test(item); },
          STRING: function(item) { return /(string|date|text|lowercase)/.test(item); },
          BOOLEAN: function() { return false; },
          FILE: function() { return false; },
          ATTACHMENT: function() { return false; }
        };
        // Add field
        $scope.addField = function() {
          let new_field = angular.copy($scope.curfield);
          $scope.curschema.fields.push(new_field);
          $scope.curfield = {name:"", store_type:"", indexed:false};
        };
        // Edit selected field
        $scope.editField = function(index) {
          $scope.editModeFlag = true;
          $scope.curindex = index;
          $scope.curfield = angular.copy($scope.curschema.fields[index]);
        };
        $scope.cancelEdit = function() {
          $scope.curfield = {name:"", store_type:"", indexed:false};
          $scope.editModeFlag = false;
          delete $scope.curindex;
        };
        $scope.saveField = function() {
          if(!$scope.curfield.index_type) { delete $scope.curfield.index_type; }
          if(!$scope.curfield.content_field) { delete $scope.curfield.content_field; }
          if(!$scope.curfield.inner_field) { delete $scope.curfield.inner_field; }
          $scope.curschema.fields[$scope.curindex] = angular.copy($scope.curfield);
          delete $scope.curindex;
          $scope.curfield = {name:"", store_type:"", indexed:false};
          $scope.editModeFlag = false;
        };
        $scope.ifsame = function() {
          return angular.equals($scope.curfield, $scope.curschema.fields[$scope.curindex]);
        };
        // Delete selected field
        $scope.deleteField = function(index) {
          $scope.curschema.fields.splice(index, 1);
        };
        // Enable index type 
        $scope.checkIndexType = function() {
          if (!($scope.curfield.indexed || $scope.curfield.content_field)) {
            $scope.curfield.index_type = null;
          }
        };
        // Diff the edited schema
        $scope._diffSchema = function() {
          // Search changed and added fields
          for(let cfield of $scope.curschema.fields) {
            let existed_flag = false;
            for(let rfield of $scope.page.table.schema.fields){
              if(cfield.name === rfield.name) {
                existed_flag = true;
                if(!angular.equals(cfield, rfield)) {
                  $scope.request_list.update_field.push({command:"update_field", table: $scope.page.table.name, field: cfield});
                }
                break;
              }
            }
            if(!existed_flag) {
              $scope.request_list.add_field.push({command:"add_field", table: $scope.page.table.name, field: cfield});
            }
          }
          // Search deleted fields
          for(let rfield of $scope.page.table.schema.fields) {
            let existed_flag = false;
            for(let cfield of $scope.curschema.fields){
              if(rfield.name === cfield.name) {
                existed_flag = true;
                break;
              }
            }
            if(!existed_flag) {
              $scope.request_list.delete_field.push({command:"delete_field", table: $scope.page.table.name, field: {name: rfield.name}});
            }
          }
        };
        // Check inputs for edit-save button
        $scope.checkEditTable = function() {
          if ($scope.curschema.fields === null || $scope.curschema.fields.length === 0) {
            //$scope.modalmsg = "No fields of schema defined!";
            $scope.modalmsg = $translate.instant('MODALMSG_NO_FIELDS');
            return false;
          } else {
            $scope.modalmsg = "";
            return true;
          }
        };
        $scope.ok = function() {
          $ngConfirm({
            title: confirmation_text,
            content: edit_table_text,
            closeIcon: true,
            scope: $scope,
            buttons: {
              Yes: {
                text: yes_text,
                action: function(scope) {
                  scope._diffSchema();
                  console.log($scope.request_list);
                }
              },
              No: {
                text: no_text,
              }
            }
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
      title: confirmation_text,
      content: delete_table_text,
      closeIcon: true,
      buttons: {
        Yes: {
          text: yes_text,
          action: function() {
            $http.post(GLOBAL.host+"/table/delete", {name:$scope.page.table.name}).then(function() {
              $scope.initial();
            });
          }
        },
        No: {
          text: no_text
        }
      }
    });
  };
  // Select table
  $scope.selectTable = function(table, index){
    $http.get(GLOBAL.host+"/schema/get", {params:{type:"table",name:table}}).then(function(data) {
      $scope.page.table.name = table;
      $scope.page.table.schema = data.data.schema;
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
