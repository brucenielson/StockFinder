/*
angular.module('divApp', [])
  .controller('DivendScoutController', function($scope, $http) {
    var divData = this;
*/
var app = angular.module('divApp', []);

app.controller('DivendScoutController', function($scope, $http) {
  divData = this;
  //$http.get("http://www.w3schools.com/angular/customers.php")
  $http.get("http://127.0.0.1:5000/data/SNP")
  //$http.get("SNP.json")
    .success(function(response) {
      //divData.names = response.records;
      //console.log(response);
      divData.stocks = response['records']
    })
    .error(function(data, status, headers, config) {
      console.log("Error");
      console.log("data: " + data);
      console.log("status: " + status);
      console.log("header: " + headers);
      console.log("config: " + config);
    });
   });


  /*
    $http.get('http://127.0.0.1/data/SNP').then(function(response) {
            console.log("here")
            //divData.stocks = response.data;
          });

    /*
    [
      {Symbol: 'TTG', Price: 22.23, Low: 20.92, High: 35, TTYield: 0.04, Projected: 0.05, Bonus: 0.03, EPS: 2.6, Payout: 0.65, Warn:true, Div:3.01, Adjusted:2.46},
      {Symbol:'H', Price:22.23, Low:20.92, High:35, TTYield:0.04, Projected:0.05, Bonus:0.03, EPS: 2.6, Payout:0.65, Warn:true, Div:3.01, Adjusted:2.46},
      {Symbol:'Q', Price:22.23, Low:20.92, High:35, TTYield:0.04, Projected:0.05, Bonus:0.03, EPS: 2.6, Payout:0.65, Warn:true, Div:3.01, Adjusted:2.46},
      {Symbol:'GH', Price:22.23, Low:20.92, High:35, TTYield:0.04, Projected:0.05, Bonus:0.03, EPS: 2.6, Payout:0.65, Warn:true, Div:3.01, Adjusted:2.46},
     ];
    */

    /*
    for(key in divData.stocks)
    {
        console.log("key " + key
            + " has value "
            + divData.stocks[0][key]);
    }
    */

    /*
    todoList.addTodo = function() {
      todoList.todos.push({text:todoList.todoText, done:false});
      todoList.todoText = '';
    };

    todoList.remaining = function() {
      var count = 0;
      angular.forEach(todoList.todos, function(todo) {
        count += todo.done ? 0 : 1;
      });
      return count;
    };

    todoList.archive = function() {
      var oldTodos = todoList.todos;
      todoList.todos = [];
      angular.forEach(oldTodos, function(todo) {
        if (!todo.done) todoList.todos.push(todo);
      });
    };
    */
//});
