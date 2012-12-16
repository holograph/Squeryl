package org.squeryl.test

import org.squeryl.framework.{SchemaTester, RunTestsInsideTransaction}
import org.squeryl.PrimitiveTypeMode._
import org.squeryl.test.schooldb._
import org.scalatest.matchers.ShouldMatchers

/**
 * Created by tomer on 12/16/12.
 */
abstract class RawQueryTest extends SchoolDbTestBase {

  test("Raw SQL can be succesfully converted into a query on a schema entity") {
    val instance = sharedTestInstance; import instance._

    val q = rawQuery( schema.students,
      "select s.* from t_student s where s.name = ? and s.age = ?", xiao.name, xiao.age )
    val result = q.headOption
    result should equal( Some( xiao ) )
  }

//  test("Raw SQL can be succesfully converted into a query of tuple") {
//    val instance = sharedTestInstance; import instance._
//
//    val q = rawQuery[ ( String, Int ) ]( "select s.name, s.age from t_student s where s.name = ?", xiao.name )
//    val (name, age) = q.head
//    name should equal( xiao.name )
//    age should equal( xiao.age )
//  }
}
