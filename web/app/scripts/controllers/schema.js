'use strict';

angular.module('basic').controller('SchemaCtrl', ['$scope', '$http', '$q', 'GLOBAL', '$uibModal', '$ngConfirm', '$translate', '$rootScope', '$stateParams', function ($scope, $http, $q, GLOBAL, $uibModal, $ngConfirm, $translate, $rootScope, $stateParams) {

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
  // ID formatter
  $scope.id_formatter_type = ["com.ngdata.hbaseindexer.uniquekey.HexUniqueKeyFormatter", "com.ngdata.hbaseindexer.uniquekey.StringUniqueKeyFormatter"];
  // get index display info by index val
  $scope.schemaIndexType = function(index) {
    for (let item of $scope.index_type) {
      if (item.val === index) {
        return item.display;
      }
    }
    return null;
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
          $scope.new_field = { name: null, store_type: null, indexed: false, index_stored: false };
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
            if(!$scope.new_field.hbase_family) { delete $scope.new_field.hbase_family; }
            if(!$scope.new_field.hbase_column) { delete $scope.new_field.hbase_column; }
            $scope.newschema.fields.push($scope.new_field);
            $scope.new_field = { name: null, store_type: null, indexed: false, index_stored: false };
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
          $scope.limitWeight = function() {
            if (!angular.isDefined($scope.new_query_field.weight) || $scope.new_query_field.weight < 0) {
              $scope.new_query_field.weight = 0;
            }
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
            if ($scope.newschema.name==="" || ($scope.newschema.index_type !== 1 && $scope.newschema.index_type !== 0 && $scope.newschema.index_type !== -1 && $scope.newschema.index_type !== 2)) {
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
          $scope.enableAddField = function() {
            if ($scope.new_field.name && $scope.new_field.store_type && ((($scope.new_field.indexed || $scope.new_field.content_field) && $scope.new_field.index_type) || !($scope.new_field.indexed || $scope.new_field.content_field))) {
              if ($scope.newschema.with_hbase) {
                return ($scope.new_field.hbase_family && $scope.new_field.hbase_column);
              } else {
                return true;
              }
            } else {
              return false;
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
      {name:"", with_hbase: false, rowkey_expression:"", table_expression:"", index_type:0, id_formatter: $scope.id_formatter_type[1], content_fields:[], inner_fields:[], fields:[], query_fields:[]},
      function() {
        $http.post(GLOBAL.host+'/schema/add', this.newschema).then(function(data){
          if (data.data.result.error_code !== 0) {
            $ngConfirm({
              title: $translate.instant('CONFIRM_TITLE_CREATE_SCHEMA_ERROR'),
              content: data.data.result.error_desc,
              closeIcon: true,
              buttons: {
                OK: {
                  text: ok_text
                }
              }
            });
          }
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
    if (!angular.isDefined($scope.schema_display[$scope.page.schema.name])) {
      $scope.schema_display[$scope.page.schema.name] = {};
      $scope.page.schema_display = false;
      $scope.page.schema_display_indeter = false;
    } else {
      let show_count = 0;
      for (let f of $scope.page.schema.fields) {
        if ($scope.schema_display[$scope.page.schema.name][f.name]) {
          show_count += 1;
        }
      }
      if (show_count === $scope.page.schema.fields.length) {
        $scope.page.schema_display = true;
        $scope.page.schema_display_indeter = false;
      } else if (show_count === 0) {
        $scope.page.schema_display = false;
        $scope.page.schema_display_indeter = false;
      } else {
        $scope.page.schema_display = false;
        $scope.page.schema_display_indeter = true;
      }
    }
  };
  $scope.checkDisplay = function() {
    if ($scope.page.schema_display_indeter && $scope.page.schema_display) {
      // Indeterminate -> All
      for(let f of $scope.page.schema.fields) {
        $scope.schema_display[$scope.page.schema.name][f.name] = true;
      }
      $scope.page.schema_display = true;
      $scope.page.schema_display_indeter = false;
    } else if (!$scope.page.schema_display && !$scope.page.schema_display_indeter) {
      // All -> None
      for(let f of $scope.page.schema.fields) {
        $scope.schema_display[$scope.page.schema.name][f.name] = false;
      }
    } else if ($scope.page.schema_display && !$scope.page.schema_display_indeter) {
      // None -> All
      for(let f of $scope.page.schema.fields) {
        $scope.schema_display[$scope.page.schema.name][f.name] = true;
      }
    }
  };
  $scope.changeDisplay = function() {
    let check_count = 0;
    for (let f of $scope.page.schema.fields) {
      if ($scope.schema_display[$scope.page.schema.name][f.name]) {
        check_count += 1;
      }
    }
    if (check_count === $scope.page.schema.fields.length) {
      $scope.page.schema_display = true;
      $scope.page.schema_display_indeter = false;
    } else if (check_count === 0) {
      $scope.page.schema_display = false;
      $scope.page.schema_display_indeter = false;
    } else {
      $scope.page.schema_display = false;
      $scope.page.schema_display_indeter = true;
    }
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
  // Initial load function
  $scope.initial = function() {
    $scope.page = {
      schema: {},
      schemasActive: [],
      schema_display: false,
      schema_display_indeter: false,
    };
    $scope.schema_display = {};
    $rootScope.global.tab = "schema";
    $q.all([$http.get(GLOBAL.host + "/schema/list"), $http.get("/schema/config")]).then(function(data) {
      $scope.schemas = data[0].data.schemas;
      $scope.schema_display = data[1].data;
      if (!$stateParams.linkschema || $stateParams.linkschema === "") {
        $scope.selectSchema($scope.schemas[0],0);
      } else {
        for (let i = 0; i < $scope.schemas.length; i++) {
          if ($scope.schemas[i].name === $stateParams.linkschema) {
            $scope.selectSchema($scope.schemas[i], i);
            break;
          }
        }
      }
    });
  };

  // Initial work
  $scope.initial();

  $scope.$on("$destroy", function() {
    if ($scope.schema_display && Object.keys($scope.schema_display).length !== 0) {
      $http.post("/schema/config/set", $scope.schema_display, function() {});
    }
  });
}])
.directive('indeterminate', function() {
  return {
    restrict: 'A',

    link(scope, elem, attr) {
      var watcher = scope.$watch(attr.indeterminate, function(value) {
        elem[0].indeterminate = value;
      });

      scope.$on('$destory', function() {
        watcher();
      });
    }
  };
});