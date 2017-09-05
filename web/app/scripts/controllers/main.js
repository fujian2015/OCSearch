'use strict';

/**
 * Main Controller
 */
angular.module('basic').controller('MainCtrl', ['$scope', 'searchService', 'hotkeys', '$state', '$rootScope', '$window', function ($scope, searchService, hotkeys, $state, $rootScope, $window) {
  $rootScope.global = {
    tab: 'search'
  };
  $scope.search = function(){
    searchService.search($scope.content, function(schemas){
      $state.go("result", {"schemas": schemas, "content": $scope.content});
    });
  };

  hotkeys.bindTo($scope)
    .add({
      combo: 'enter',
      allowIn: ['INPUT', 'SELECT', 'TEXTAREA'],
      callback: $scope.search
    });
  
  let focusElem = $window.document.getElementById('mainSearchInput');
  if (focusElem) {
    focusElem.focus();
  }
}]);
