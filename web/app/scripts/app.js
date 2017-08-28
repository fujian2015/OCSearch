'use strict';

/**
 * @ngdoc overview
 * @name basicApp
 * @description
 * # basicApp
 *
 * Main module of the application.
 */
angular
  .module('basic', [
    'ngAnimate',
    'ngAria',
    'ngCookies',
    'ngMessages',
    'ngResource',
    'ui.router',
    'ngSanitize',
    'ngTouch',
    'pascalprecht.translate',
    'ngFileUpload',
    "isteven-multi-select",
    "dndLists",
    'ui.bootstrap',
    'ui-notification',
    'angularSpinner',
    'ngCookies',
    'ui.select',
    'toggle-switch',
    'cfp.hotkeys',
    'ui.bootstrap.datetimepicker',
    'angularMoment',
    'chart.js',
    'cp.ngConfirm'
  ])
  .config(function ($stateProvider, $urlRouterProvider, $httpProvider, NotificationProvider, $translateProvider) {
    $urlRouterProvider.otherwise("home");
    $stateProvider
      .state('home', {
        url:"/home",
        templateUrl: 'views/main.html',
        controller: 'MainCtrl'
      })
      .state('schema', {
        url:"/schema",
        templateUrl: 'views/schema.html',
        controller: 'SchemaCtrl'
      })
      .state('table', {
        url:"/table",
        templateUrl: 'views/table.html',
        controller: 'TableCtrl'
      })
      .state('result', {
        url:"/result",
        params: {"schemas": null, content: null},
        templateUrl: 'views/result.html',
        controller: 'ResultCtrl'
      });
    $httpProvider.interceptors.push('spinnerInterceptor');
    NotificationProvider.setOptions({
      delay: 10000,
      startTop: 20,
      startRight: 10,
      verticalSpacing: 20,
      horizontalSpacing: 20,
      positionX: 'right',
      positionY: 'bottom'
    });
    $translateProvider.translations("en", {
      ADD_NEW_TABLE: "Add new table",
      NAME: "Name",
      INDEX_TYPE: "Index Type",
      ROWKEY_EXPRESSION: "Rowkey Expression",
      TABLE_EXPRESSION: "Table Expression",
      SCHEMA: "Schema",
      FIELDS: "Fields",
      REGION_NUM: "Region Number",
      REGION_SPLIT: "Region Split",
      SOLR_SHARDS: "Solr Shards",
      SOLR_REPLICAS: "Solr Replicas",
      OK: "OK",
      CANCEL: "Cancel",
      NEXT: "Next",
      PREV: "Previous",
      EDIT_TABLE_FIELDS: "Edit Table Fields",
      FIELD_NAME: "Field Name:",
      STORE_TYPE: "Store Type:",
      CONTENT_FIELD: "Content Field",
      INNER_FIELD: "Inner Field",
      FIELD_INDEX: "Field Index",
      INDEXED: "Indexed",
      INTABLE_NAME: "name",
      INTABLE_TYPE: "type",
      INTABLE_SEPARATOR: "separator",
      INTABLE_STORE_TYPE: "store type",
      INTABLE_INDEX_TYPE: "index type",
      INTABLE_INNER_FIELD: "inner field",
      INTABLE_CONTENT_FIELD: "content field",
      INTABLE_QUERY_WEIGHT: "query weight",
      INTABLE_OPERATION: "operation",
      SELECT_SCHEMA: "Select schema",
      SELECT_FIELD_STORE_TYPE: "Select field store type",
      SELECT_FIELD_INDEX_TYPE: "Select field index type",
      SELECT_FIELD_CONTENT_FIELD: "Select existed content field",
      SELECT_FIELD_INNER_FIELD: "Select existed inner field",
      BASIC_INFO: "Basic Info",
      ADD: "Add",
      EDIT: "Edit",
      DELETE: "Delete",
      UNAVAILABLE: "Unavailable",
      YES: "Yes",
      NO: "No",
      CONFIRMATION: "Confirmation",
      MODALMSG_FILL_IN_ALL: "Please fill in all inputs marked as *",
      MODALMSG_NO_FIELDS: "No fields of schema defined!",
      CONFIRM_ADD_TABLE: "Create table now?",
      CONFIRM_EDIT_TABLE: "Change the schema of table now?",
      CONFIRM_DELETE_TABLE: "Want to delete the table selected? All data related will be lost!",
    }).translations("zh", {
      ADD_NEW_TABLE: "增加新表",
      NAME: "名称",
      INDEX_TYPE: "索引类型",
      ROWKEY_EXPRESSION: "Rowkey Expression",
      TABLE_EXPRESSION: "Table Expression",
      SCHEMA: "表结构",
      FIELDS: "字段",
      REGION_NUM: "Region Num",
      REGION_SPLIT: "Region Split",
      SOLR_SHARDS: "Solr Shards",
      SOLR_REPLICAS: "Solr Replicas",
      OK: "确定",
      CANCEL: "取消",
      NEXT: "下一步",
      PREV: "上一步",
      EDIT_TABLE_FIELDS: "编辑表结构",
      FIELD_NAME: "字段名",
      STORE_TYPE: "存储类型",
      CONTENT_FIELD: "Content Field",
      INNER_FIELD: "Inner Field",
      FIELD_INDEX: "字段索引",
      INDEXED: "索引",
      INTABLE_NAME: "字段名",
      INTABLE_TYPE: "字段类型",
      INTABLE_SEPARATOR: "分隔符",
      INTABLE_STORE_TYPE: "字段类型",
      INTABLE_INDEX_TYPE: "字段索引类型",
      INTABLE_INNER_FIELD: "inner field",
      INTABLE_CONTENT_FIELD: "content field",
      INTABLE_QUERY_WEIGHT: "query weight",
      INTABLE_OPERATION: "操作",
      SELECT_SCHEMA: "选择表结构",
      SELECT_FIELD_STORE_TYPE: "选择字段存储类型",
      SELECT_FIELD_INDEX_TYPE: "选择索引类型",
      SELECT_FIELD_CONTENT_FIELD: "选择已经存在的content字段",
      SELECT_FIELD_INNER_FIELD: "选择已经存在的inner字段",
      BASIC_INFO: "基础信息",
      ADD: "增加",
      EDIT: "编辑",
      DELETE: "删除",
      UNAVAILABLE: "不可用",
      YES: "是",
      NO: "否",
      CONFIRMATION: "确定",
      MODALMSG_FILL_IN_ALL: "请填写所有标记为*的内容",
      MODALMSG_NO_FIELDS: "未定义任何字段！",
      CONFIRM_ADD_TABLE: "新建表？",
      CONFIRM_EDIT_TABLE: "改变表结构？",
      CONFIRM_DELETE_TABLE: "删除选中表？表中所有数据将会丢失！",
    });
    $translateProvider.preferredLanguage('zh');
  }).constant('GLOBAL', {
    host: './ocsearch-service',
  }).run(['$rootScope', function ($rootScope) {
    $rootScope.global = {
      tab: null
    };
    $rootScope.functions = {};
    $rootScope.functions.click = function(item){
      $rootScope.global.tab = item;
    };
  }]);
