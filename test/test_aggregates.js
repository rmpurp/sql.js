exports.test = function(SQL, assert){
  var db = new SQL.Database();
  db.exec("CREATE TABLE test (num, str);");
  db.exec('INSERT INTO test VALUES (1, "h"), (1, "e"), (4, "llo "), (6, "world!");');

  db.exec("CREATE TABLE dual_numbers (x, y);");
  db.exec("INSERT INTO dual_numbers VALUES (1, 4), (5, 6), (2, -4);");
  
  db.exec("CREATE TABLE customers (name, type, income);");
  db.exec('INSERT INTO customers VALUES ("bob", "normal", 300000), '
    + '("mary", "normal" , 400000), ("jane", "pro", 450000), ("esther", "pro", 810000)');


  // replicating sum aggregate
  function summation_step(x, state) {
      if (state == null) {
          return x;
      } else {
          return state + x; // state will be the last returned value
      }
  };
  
  function summation_finalize(state) {
      return state;
  }
  // Register with SQLite.
  db.create_aggregate("summation", summation_step, summation_finalize);

  // Use in a query, check expected result.
  var result = db.exec("SELECT summation(num) FROM test;");
  var result_num = result[0]["values"][0][0];

  assert.equal(result_num, 12, "Named aggregates can be registered");

  result = db.exec("SELECT summation(str) FROM test;");
  var result_str = result[0]["values"][0][0];
  assert.equal(result_str, "hello world!", "Javascript operator overloading works");



  function summation_max_step(x, y, state) {
      if (state == null) {
          return [x, y]
      } else {
          return [state[0] + x, state[1] + y]
      }
  } 
  function summation_max_final(state) {
      return state[0] > state[1] ? state[0] : state[1];
  }
  
  
  db.create_aggregate("sum_max", summation_max_step, summation_max_final);

  result = db.exec("SELECT sum_max(x, y) FROM dual_numbers;");
  result_num = result[0]["values"][0][0];
  assert.equal(result_num, 8, "Aggregates with multiple arguments")
  

  // replicating avg aggregate
  function mean_step(x, state) {
      if (state == null) {
          return [x, 1];
      } else {
          return [x + state[0], state[1] + 1]
      }
  };
  
  function mean_finalize(state) {
      return state[0] / state[1]
  }

  db.create_aggregate("mean", mean_step, mean_finalize);

  result = db.exec("SELECT type, mean(income) FROM customers GROUP BY type HAVING summation(income) > 1000000;")
    

  result_str = result[0]["values"][0][0];
  result_num = result[0]["values"][0][1];
  
  assert.equal(result_num, 630000, "Custom aggregate in HAVING clause")
  assert.equal(result_str, "pro", "Custom aggregate in HAVING clause")
  
  result = db.exec("SELECT mean(income), summation(income) FROM customers;");
  var result_mean = result[0]["values"][0][0];
  var result_sum = result[0]["values"][0][1];

  assert.equal(result_mean, 490000, "Multiple aggregates in selection");
  assert.equal(result_sum, 490000 * 4, "Multiple aggregates in selection");

  db.close();
};

if (module == require.main) {
  var sql = require('../js/sql.js');
  var assert = require("assert");
  exports.test(sql, assert);
}
