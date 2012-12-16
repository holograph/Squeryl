package org.squeryl.dsl.boilerplate

import org.squeryl.{Session, Query, Table}
import org.squeryl.dsl.AbstractQuery
import org.squeryl.dsl.ast.{FieldSelectElement, ExpressionNode}
import org.squeryl.internals.{ResultSetUtils, Utils, ResultSetMapper, StatementWriter}
import java.sql.ResultSet
import java.io.Closeable
import org.squeryl.logging.StatementInvocationEvent

/**
 * Created by tomer on 12/17/12.
 */

trait RawQuerySignatures {
  private class RawQuery[R](private val t: Table[R], private val sql: String,
                            private val args: Seq[AnyRef], isRoot: Boolean)
    extends Query[R] {

    private val _resultSetMapper = new ResultSetMapper

    def iterator = new Iterator[R] with Closeable {
      val sw = new StatementWriter(false, Session.currentSession.databaseAdapter)
      ast.write(sw)
      val s = Session.currentSession
      val beforeQueryExecute = System.currentTimeMillis
      val (rs, stmt) = Session.currentSession.databaseAdapter.executeQuery(s, sw)

//      lazy val statEx = new StatementInvocationEvent(definitionSite.get, beforeQueryExecute, System.currentTimeMillis, -1, sw.statement)
//
//      if(s.statisticsListener != None)
//        s.statisticsListener.get.queryExecuted(statEx)

      s._addStatement(stmt) // if the iteration doesn't get completed, we must hang on to the statement to clean it up at session end.
      s._addResultSet(rs) // same for the result set

      var _nextCalled = false
      var _hasNext = false

      var rowCount = 0

      def close() {
        stmt.close()
        rs.close()
      }

      def _next() {
        _hasNext = rs.next

        if(!_hasNext) {// close it since we've completed the iteration
          Utils.close(rs)
          stmt.close()

//          if(s.statisticsListener != None) {
//            s.statisticsListener.get.resultSetIterationEnded(statEx.uuid, System.currentTimeMillis, rowCount, true)
//          }
        }

        rowCount = rowCount + 1
        _nextCalled = true
      }

      def hasNext = {
        if(!_nextCalled)
          _next()
        _hasNext
      }

      def next: R = {
        if(!_nextCalled)
          _next()
        if(!_hasNext)
          throw new NoSuchElementException("next called with no rows available")
        _nextCalled = false

        if(s.isLoggingEnabled)
          s.log(ResultSetUtils.dumpRow(rs))

        give(_resultSetMapper, rs)
      }
    }

    def dumpAst = // TODO
      throw new UnsupportedOperationException("To be completed...")

    def statement = // TODO
      throw new UnsupportedOperationException("To be completed...")

    private[squeryl] def copy(asRoot: Boolean) = new RawQuery(t, sql, args, asRoot)

    def distinct = throw new UnsupportedOperationException("Modifiers are not supported on raw queries")

    def forUpdate = throw new UnsupportedOperationException("Modifiers are not supported on raw queries")

    def page(offset: Int, pageLength: Int) =    // Maybe support in the future? --TG
      throw new UnsupportedOperationException("Modifiers are not supported on raw queries")

    def name = "rawQuery"

    private[squeryl] def give(resultSetMapper: ResultSetMapper, rs: ResultSet) = {
//      resultSetMapper.pushYieldedValues(rs)   // ? --TG
      for((fmd, i) <- t.posoMetaData.fieldsMetaData.zipWithIndex) {
        val jdbcIndex = i + 1
        val fse = new FieldSelectElement(null, fmd, resultSetMapper)
        fse.prepareColumnMapper(jdbcIndex)
        fse.prepareMapper(jdbcIndex)
      }
      t.give(resultSetMapper, rs)
    }

    val ast = new ExpressionNode {
      def doWrite(sw: StatementWriter) { sw.write(sql); args foreach sw.addParam }
    }

    protected[squeryl] def invokeYield(rsm: ResultSetMapper, resultSet: ResultSet) =
      throw new Exception("To be completed")
  }

  def rawQuery[R](t: Table[R], sql: String, args: AnyRef*): Query[R] = new RawQuery(t, sql, args, true)
}
