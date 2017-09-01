'use strict';

angular.module('basic').controller('SchemaCtrl', ['$scope', '$http', 'GLOBAL', '$uibModal', '$ngConfirm', '$translate', '$state', '$rootScope', function ($scope, $http, GLOBAL, $uibModal, $ngConfirm, $translate, $state, $rootScope) {

  let yes_text = $translate.instant('YES');
  let no_text = $translate.instant('NO');
  let ok_text = $translate.instant('OK');
  let warn_text = $translate.instant('WARNING');
  let confirmation_text = $translate.instant('CONFIRMATION');

  //---------- Tool functions ----------
  // 1, queryWeight
  // 2, schemaIndexType
  // 3, joinFields
  // 4, modalAction
  // Get weight if queried
  $scope.queryWeight = function(field, query_fields) {
    for (let qfield of query_fields) {
      if (field === qfield.name) {
        return qfield.weight;
      }
    }
    return null;
  };
  // schema index type
  $scope.index_type = [
    { val: 0, display: "hbase+indexer+solr" },
    { val: -1, display: "hbase only" },
    { val: 1, display: "hbase+phoenix" },
    { val: 2, display: "hbase+phoenix+solr" }
  ];
  // get index display info by index val
  $scope.schemaIndexType = function(index) {
    for (let item of $scope.index_type) {
      if (item.val === index) {
        return item.display;
      }
    }
    return null;
  };
  // output content_fields and inner_fields in format string
  $scope.joinFields = function(fields) {
    if (angular.isDefined(fields)) {
      let farr = [];
      for (let f of fields) {
        if (!f.hasOwnProperty("name")) { continue; }
        let fstr = [];
        fstr.push(f.name);
        fstr.push("(");
        for (let k in f) {
          if (k === "name") { continue; }
          fstr.push(k+":"+f[k]);
        }
        fstr.push(")");
        farr.push(fstr.join(''));
      }
      return farr.join(',');
    } else {
      return null;
    }
  };
  // Template modal factory
  $scope.modalAction = function(title, confirm_content, initval, okfunc) {
    let returnfunc = function(title, confirm_content, initval, okfunc) {
      let modalInstance = $uibModal.open({
        animation: true,
        templateUrl: 'addSchema.html',
        backdrop: 'static',
        scope: $scope,
        size: 'lg',
        controller: ['$scope', '$http', '$ngConfirm', '$sce', function($scope, $http, $ngConfirm, $sce) {
          $scope.helpExpressionHtml = $sce.trustAsHtml('<h5>'+$translate.instant('HELP_EXPRESSION_TITLE')+'</h5>'+$translate.instant('HELP_EXPRESSION_CONTENT')+'<a target="_blank" href="expression.html">'+$translate.instant('HELP_EXPRESSION_LINK')+'</a>?');
          $scope.titleStr = title;
          // Temp new schema
          $scope.newschema = initval;
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
          // Temp new content field
          $scope.new_content_field = { name: null, type: null };
          // Temp new inner field
          $scope.new_inner_field = { name: null, separator: null };
          // Temp new field
          $scope.new_field = { name: null, store_type: null, indexed: true, index_stored: false };
          // Temp new query field
          $scope.new_query_field = { name: null, weight: null };
          // Functions of Content Field
          $scope.addContentField = function() {
            $scope.newschema.content_fields.push($scope.new_content_field);
            $scope.new_content_field = { name: null, type: null };
          };
          $scope.removeContentField = function($index) {
            $scope.newschema.content_fields.splice($index, 1);
          };
          // Functions of Inner Field
          $scope.addInnerField = function() {
            $scope.newschema.inner_fields.push($scope.new_inner_field);
            $scope.new_inner_field = { name: null, separator: null };
          };
          $scope.removeInnerField = function($index) {
            $scope.newschema.inner_fields.splice($index, 1);
          };
          // Functions of Define Field
          $scope.addField = function() {
            if(!$scope.new_field.index_type) { delete $scope.new_field.index_type; }
            if(!$scope.new_field.content_field) { delete $scope.new_field.content_field; }
            if(!$scope.new_field.inner_field) { delete $scope.new_field.inner_field; }
            $scope.newschema.fields.push($scope.new_field);
            $scope.new_field = { name: null, store_type: null, indexed: true, index_stored: false };
          };
          $scope.removeField = function($index) {
            $scope.newschema.fields.splice($index, 1);
          };
          $scope.checkIndexType = function() {
            if (!($scope.new_field.indexed || $scope.new_field.content_field)) {
              $scope.new_field.index_type = null;
            }
          };
          // Functions of Query Field
          $scope.addQueryField = function() {
            $scope.newschema.query_fields.push($scope.new_query_field);
            $scope.new_query_field = { name: null, weight: null };
          };
          $scope.removeQueryField = function($index) {
            $scope.newschema.query_fields.splice($index, 1);
          };
          // Functions of Add Schema steps
          $scope.addsteps = ["step1", "step2", "step3"];
          $scope.curstep = 0;
          $scope.next = function() {
            $scope.curstep = ($scope.curstep + 1) % $scope.addsteps.length;
          };
          $scope.prev = function() {
            $scope.curstep = ($scope.curstep - 1) % $scope.addsteps.length;
          };
          $scope._ok = okfunc;
          $scope.ok = function() {
            $ngConfirm({
              title: title,
              content: confirm_content,
              scope: $scope,
              closeIcon: true,
              buttons: {
                Yes: {
                  text: yes_text,
                  action: function(scope) {
                    scope._ok();
                    modalInstance.close();
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
          // Check input parameters
          $scope.checkStep1 = function() {
            if ($scope.newschema.name==="" || ($scope.newschema.index_type !== 1 && $scope.newschema.index_type !== 0 && $scope.newschema.index_type !== -1)) {
              $scope.modalmsg = $translate.instant("MODALMSG_FILL_IN_ALL");
              return false;
            } else {
              $scope.modalmsg = "";
              return true;
            }
          };
          $scope.checkStep2 = function() {
            if ($scope.newschema.fields === null || $scope.newschema.fields.length === 0) {
              $scope.modalmsg = $translate.instant('MODALMSG_NO_FIELDS');
              return false;
            } else {
              $scope.modalmsg = "";
              return true;
            }
          };
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
          $scope.queryFilter = function(item) {
            return /^text/.test(item.index_type);
          };
        }] // END of controller
      }); // END of modal instance
    }; // END of return function
    return returnfunc(title, confirm_content, initval, okfunc);
  }; // END of template function

  //---------- Basic operation of schema ----------
  // 1, addSchema
  // 2, editSchema
  // 3, selectSchema
  // 4, deleteSchema
  // 5, initialSchema
  // Add schema
  $scope.addSchema = function() {
    $scope.modalAction(
      $translate.instant('ADD_NEW_SCHEMA'), 
      $translate.instant('CONFIRM_ADD_SCHEMA'),
      {name:"", rowkey_expression:"", table_expression:"", index_type:"", content_fields:[], inner_fields:[], fields:[], query_fields:[]},
      function() {
        $http.post(GLOBAL.host+'/schema/add', this.newschema).then(function(){
          $scope.initial();
        });
      }
    );
  };
  // Edit schema
  $scope.editSchema = function() {
    if (angular.isDefined($scope.page.tables)) {
      if ($scope.page.tables.length > 0) {
        $ngConfirm({
          title: warn_text, 
          content: $translate.instant('CONFIRM_EDIT_SCHEMA_ERR'),
          closeIcon: true,
          buttons: {
            OK: {
              text: ok_text
            }
          }
        });
      } else {
        $scope.modalAction(
          $translate.instant('EDIT_SCHEMA'), 
          $translate.instant('CONFIRM_EDIT_SCHEMA'),
          angular.copy($scope.page.schema),
          function() {
            let schema_deleted = {name:$scope.page.schema.name};
            let schema_added = angular.copy(this.newschema);
            $http.post(GLOBAL.host + "/schema/delete", schema_deleted).then(function() {
              $http.post(GLOBAL.host + "/schema/add", schema_added).then(function() {
                $scope.initial();
              });
            });
          }
        );
      }
    }
  };
  // Select schema item
  $scope.selectSchema = function(schema, index) {
    $scope.page.schema = schema;
    $scope.page.schemasActive = [];
    for (let i = 0; i < $scope.schemas.length; i++) {
      if ( i === index ) {
        $scope.page.schemasActive.push(true);
      } else {
        $scope.page.schemasActive.push(false);
      }
    }
    let schema_tables = {schema: $scope.page.schema.name};
    $http.post(GLOBAL.host + "/table/list", schema_tables).then(function(data) {
      $scope.page.tables = data.data.tables;
    });
  };
  // Delete schema
  $scope.deleteSchema = function() {
    if (angular.isDefined($scope.page.tables)) {
      if ($scope.page.tables.length > 0) {
        $ngConfirm({
          title: warn_text, 
          content: $translate.instant('CONFIRM_DELETE_SCHEMA_ERR'),
          closeIcon: true,
          buttons: {
            OK: {
              text: ok_text
            }
          }
        });
      } else { // There are not any tables belong to the schema
        $ngConfirm({
          title: confirmation_text,
          content: $translate.instant('CONFIRM_DELETE_SCHEMA'),
          closeIcon: true,
          scope: $scope,
          buttons: {
            Yes: {
              text: yes_text,
              action: function(scope) {
                scope._deleteSchema();
                //scope.$apply(); //force refresh ng-modal
              }
            },
            No: {
              text: no_text
            }
          }
        });
      }
    }
  };
  // Delete schema ajax
  $scope._deleteSchema = function() {
    let schema_deleted = {name:$scope.page.schema.name};
    $http.post(GLOBAL.host+"/schema/delete", schema_deleted).then(function() {
      $scope.initial();
    });
  };
  // Skip to table of schema
  $scope.skipToTable = function(table) {
    let paramtable = { linktable: table };
    $state.go('table', paramtable);
    $rootScope.global.tab = 'table';
  };
  //Initial load function
  $scope.initial = function() {
    $scope.page = {
      schema: {},
      schemasActive: []
    };
    $http.get(GLOBAL.host + "/schema/list").then(function(data) {
      $scope.schemas = data.data.schemas;
      $scope.selectSchema($scope.schemas[0],0);
    });
  };

  // Initial work
  $scope.initial();

}]);