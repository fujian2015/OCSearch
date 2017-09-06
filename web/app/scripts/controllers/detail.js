'use strict';

angular.module('basic').controller('DetailCtrl', ['$scope', '$q', '$http', 'GLOBAL', '$stateParams', function($scope, $q, $http, GLOBAL, $stateParams) {
  /*
  * Initial parameters
  */
  $scope.idparam = $stateParams.id;
  $scope.schemaparam = $stateParams.schema;
  $scope.tableparam = $stateParams.table;

  /*
  * Initial functions
  */
  if ($scope.idparam && $scope.schemaparam && $scope.tableparam) {
    //$http.get(GLOBAL.host + "/schema/get", {params:{type:"table", name:$scope.table}}).then(function(data) {
      //console.log(data.data);
      //$http.post(GLOBAL.host + "/query/get", {"tables":[$scope.table], "ids":[$scope.id], "return_fields":[]}).then(function(data) {
        //console.log(data.data);
      //});
    //});
    $q.all([$http.post(GLOBAL.host + "/query/get", {"tables":[$scope.tableparam], "ids":[$scope.idparam], "return_fields":[]}), $http.get(GLOBAL.host + "/schema/get", {params:{type:"table", name:$scope.tableparam}})]).then(function(data) {
      $scope.schema = data[1].data.schema;
      if (data[0].data.data.docs.length > 0) {
        $scope.item = data[0].data.data.docs[0];
      }
    });
  }

}]);