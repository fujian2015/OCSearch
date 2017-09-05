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
        controller: 'SchemaCtrl',
        params: {
          'linkschema' : ''
        }
      })
      .state('table', {
        url:"/table",
        templateUrl: 'views/table.html',
        controller: 'TableCtrl',
        params: {
          'linktable' : ''
        }
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
      ADD_NEW_SCHEMA: "Add New Schema",
      EDIT_SCHEMA: "Edit Schema",
      NAME: "Name",
      INDEX_TYPE: "Index Type",
      ROWKEY_EXPRESSION: "Rowkey Expression",
      TABLE_EXPRESSION: "Table Expression",
      SCHEMA: "Schema",
      WEIGHT: "Weight",
      FIELDS: "Fields",
      TABLE: "Table",
      SEARCH: "Search",
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
      QUERY_FIELD: "Query Field",
      INTABLE_NAME: "name",
      INTABLE_TYPE: "type",
      INTABLE_SEPARATOR: "separator",
      INTABLE_STORE_TYPE: "store type",
      INTABLE_INDEX_TYPE: "index type",
      INTABLE_INNER_FIELD: "inner field",
      INTABLE_CONTENT_FIELD: "content field",
      INTABLE_QUERY_WEIGHT: "query weight",
      INTABLE_OPERATION: "operation",
      INTABLE_WEIGHT: "weight",
      INTABLE_DISPLAY: "display in search",
      DISPLAY: "display",
      SELECT_TYPE: "Select Type",
      SELECT_SCHEMA: "Select schema",
      SELECT_INDEX_TYPE: "Select index type",
      SELECT_FIELD_STORE_TYPE: "Select field store type",
      SELECT_FIELD_INDEX_TYPE: "Select field index type",
      SELECT_FIELD_CONTENT_FIELD: "Select existed content field",
      SELECT_FIELD_INNER_FIELD: "Select existed inner field",
      SELECT_QUERY_FIELD: "Select existed field to query",
      BASIC_INFO: "Basic Info",
      ADD: "Add",
      EDIT: "Edit",
      DELETE: "Delete",
      UNAVAILABLE: "Unavailable",
      YES: "Yes",
      NO: "No",
      CONFIRMATION: "Confirmation",
      WARNING: "Warning",
      MODALMSG_FILL_IN_ALL: "Please fill in all inputs marked as *",
      MODALMSG_NO_FIELDS: "No fields of schema defined!",
      CONFIRM_ADD_TABLE: "Create table now?",
      CONFIRM_EDIT_TABLE: "Change the schema of table now?",
      CONFIRM_DELETE_TABLE: "Want to delete the table selected? All data related will be lost!",
      CONFIRM_DELETE_SCHEMA: "Are you sure to delete the schema selected?",
      CONFIRM_ADD_SCHEMA: "Create schema now?",
      CONFIRM_EDIT_SCHEMA: "Recreate schema now?",
      CONFIRM_DELETE_SCHEMA_ERR: "There are tables belong to the schema. Delete all related tables first of all!",
      CONFIRM_EDIT_SCHEMA_ERR: 'There are tables belong to the schema. You can just edit fields of schema in "Table" panel.',
      CONFIRM_TITLE_CREATE_TABLE_ERROR: "Error occur during creation of table",
      EMPTY: "(empty)",
      UNDEFINED: "(undefined)",
      HELP_EXPRESSION_TITLE: "Help to build an expression",
      HELP_EXPRESSION_CONTENT: "Want to learn",
      HELP_EXPRESSION_LINK: "more",
      ROWS: "Rows",
      CONDITIONS: "Conditions",
    }).translations("zh", {
      ADD_NEW_TABLE: "增加新表",
      ADD_NEW_SCHEMA: "新建表结构",
      EDIT_SCHEMA: "编辑表结构",
      NAME: "名称",
      INDEX_TYPE: "索引类型",
      ROWKEY_EXPRESSION: "Rowkey表达式",
      TABLE_EXPRESSION: "Table表达式",
      SCHEMA: "表结构",
      WEIGHT: "权重",
      FIELDS: "字段",
      TABLE: "表",
      SEARCH: "搜索",
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
      CONTENT_FIELD: "默认搜索字段",
      INNER_FIELD: "压缩字段",
      FIELD_INDEX: "字段索引",
      INDEXED: "索引",
      QUERY_FIELD: "查询字段",
      INTABLE_NAME: "字段名",
      INTABLE_TYPE: "字段类型",
      INTABLE_SEPARATOR: "分隔符",
      INTABLE_STORE_TYPE: "字段类型",
      INTABLE_INDEX_TYPE: "字段索引类型",
      INTABLE_INNER_FIELD: "压缩字段",
      INTABLE_CONTENT_FIELD: "默认搜索字段",
      INTABLE_QUERY_WEIGHT: "查询权重",
      INTABLE_OPERATION: "操作",
      INTABLE_WEIGHT: "权重",
      INTABLE_DISPLAY: "搜索结果显示",
      DISPLAY: "显示",
      SELECT_TYPE: "选择类型",
      SELECT_SCHEMA: "选择表结构",
      SELECT_INDEX_TYPE: "选择索引类型",
      SELECT_FIELD_STORE_TYPE: "选择字段存储类型",
      SELECT_FIELD_INDEX_TYPE: "选择索引类型",
      SELECT_FIELD_CONTENT_FIELD: "选择已经存在的默认搜索字段",
      SELECT_FIELD_INNER_FIELD: "选择已经存在的压缩字段",
      SELECT_QUERY_FIELD: "选择已经存在的字段建立查询字段",
      BASIC_INFO: "基础信息",
      ADD: "新建",
      EDIT: "编辑",
      DELETE: "删除",
      UNAVAILABLE: "不可用",
      YES: "是",
      NO: "否",
      CONFIRMATION: "确定",
      WARNING: "警告",
      MODALMSG_FILL_IN_ALL: "请填写所有标记为*的内容",
      MODALMSG_NO_FIELDS: "未定义任何字段！",
      CONFIRM_ADD_TABLE: "新建表？",
      CONFIRM_EDIT_TABLE: "改变表结构？",
      CONFIRM_DELETE_TABLE: "删除选中表？表中所有数据将会丢失！",
      CONFIRM_ADD_SCHEMA: "确定新建表结构？",
      CONFIRM_EDIT_SCHEMA: "确定修改表结构？",
      CONFIRM_DELETE_SCHEMA: "确定删除选中的表结构？",
      CONFIRM_DELETE_SCHEMA_ERR: "无法删除表结构，请先删除所有相关的表！",
      CONFIRM_EDIT_SCHEMA_ERR: '无法编辑表结构，请在"表"面板里编辑表结构字段',
      CONFIRM_TITLE_CREATE_TABLE_ERROR: "创建表失败",
      EMPTY: "(空)",
      UNDEFINED: "(未定义)",
      HELP_EXPRESSION_TITLE: "如何建立一个表达式",
      HELP_EXPRESSION_CONTENT: "想要了解",
      HELP_EXPRESSION_LINK: "更多",
      ROWS: "每页行数",
      CONDITIONS: "搜索条件",
    });
    $translateProvider.preferredLanguage('zh');
  }).constant('GLOBAL', {
    host: './ocsearch-service',
  }).run(['$rootScope', function ($rootScope) {
    $rootScope.global = {
      tab: 'search'
    };
    $rootScope.functions = {};
    $rootScope.functions.click = function(item){
      $rootScope.global.tab = item;
    };
  }]);
