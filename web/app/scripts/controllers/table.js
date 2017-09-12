'use strict';

angular.module('basic').controller('TableCtrl', ['$scope', '$http', '$q', 'GLOBAL', '$uibModal', '$ngConfirm', '$translate', '$stateParams', '$rootScope', function ($scope, $http, $q, GLOBAL, $uibModal, $ngConfirm, $translate, $stateParams, $rootScope) {
  let yes_text = $translate.instant('YES');
  let no_text = $translate.instant('NO');
  let ok_text = $translate.instant('OK');
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
              region_num: 4,
              region_split: []
            },
            solr:{
              shards: 1,
              replicas: 1
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
          $scope.changeSchema = function() {
            for(let schema of $scope.schemas) {
              if(schema.name === $scope.newtable.schema) {
                if (!schema.with_hbase) {
                  $scope.newtable.hbase.region_num = 0;
                  return;
                } else {
                  $scope.newtable.hbase.region_num = 4;
                  return;
                }
              }
            }
          };
          $scope._ok = function(){
            if ($scope.newtable.hbase.region_split === "" || $scope.newtable.hbase.region_split === null) { $scope.newtable.hbase.region_split = []; }
            $http.post(GLOBAL.host+"/table/create", $scope.newtable).then(function(data) {
              if (data.data.result.error_code !== 0) {
                $ngConfirm({
                  title: $translate.instant('CONFIRM_TITLE_CREATE_TABLE_ERROR'),
                  content: data.data.result.error_desc,
                  scope: $scope,
                  closeIcon: true,
                  buttons: {
                    OK: {
                      text: ok_text
                    }
                  }
                });
              }
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
        $scope.request_list = [];
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
                  $scope.request_list.push({command:"update_field", table: $scope.page.table.name, field: cfield});
                }
                break;
              }
            }
            if(!existed_flag) {
              $scope.request_list.push({command:"add_field", table: $scope.page.table.name, field: cfield});
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
              $scope.request_list.push({command:"delete_field", table: $scope.page.table.name, field: {name: rfield.name}});
            }
          }
        };
        $scope._ok = function() {
          let updateTable = i => $http.post(GLOBAL.host+"/schema/update", $scope.request_list[i]).then(function(data) {
            $scope.request_list[i].return_code = data.data.result.error_code;
            $scope.request_list[i].return_desc = data.data.result.error_desc;
          });
          let promises = [];
          for (let i = 0; i < $scope.request_list.length; i++) {
            promises.push(updateTable(i));
          }
          $q.all(promises).then(function() {
            let err_flag = false;
            let result_lst = [];
            for (let item of $scope.request_list) {
              if (item.return_code !== 0) {
                err_flag = true;
                result_lst.push("Action:" + item.command + " of " + item.field.name + "   Error:" + item.return_desc);
              }
            }
            if (err_flag) {
              $ngConfirm({
                title: $translate.instant('CONFIRM_TITLE_EDIT_TABLE_ERROR'),
                content: result_lst.join('\n'),
                closeIcon: true,
                scope: $scope,
                buttons: {
                  OK: {
                    text: ok_text
                  }
                }
              });
            }
            $scope.initial();
            modalInstance.close();
          });
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
                  scope._ok();
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
    $rootScope.global.tab = "table";
    $http.get(GLOBAL.host+"/table/list").then(function(data) {
      $scope.tables = data.data.tables;
      if (!$stateParams.linktable || $stateParams.linktable === '') {
        $scope.selectTable($scope.tables[0], 0);
      } else {
        let table_index = $scope.tables.indexOf($stateParams.linktable);
        if (table_index >= 0 && table_index < $scope.tables.length) {
          $scope.selectTable($scope.tables[table_index], table_index);
        } else {
          $scope.selectTable($scope.tables[0], 0);
        }
      }
    });
  };

  // initial steps
  $scope.initial();
}]);
