/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;

import org.antlr.runtime.*;
import org.apache.cassandra.cql3.hooks.OnPrepareHook;
import org.apache.cassandra.cql3.hooks.PostExecutionHook;
import org.apache.cassandra.cql3.hooks.PreExecutionHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SemanticVersion;

public class QueryProcessor
{
    public static final SemanticVersion CQL_VERSION = new SemanticVersion("3.0.0-beta1");

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    private static volatile PreExecutionHook preExecutionHook = PreExecutionHook.NO_OP;
    private static volatile PostExecutionHook postExecutionHook = PostExecutionHook.NO_OP;
    private static volatile OnPrepareHook onPrepareHook = OnPrepareHook.NO_OP;

    public static void setPreExecutionHook(PreExecutionHook hook)
    {
        preExecutionHook = hook;
    }

    public static void setPostExecutionHook(PostExecutionHook hook)
    {
        postExecutionHook = hook;
    }

    public static void setOnPrepareHook(OnPrepareHook hook)
    {
        onPrepareHook = hook;
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public static void validateColumnNames(Iterable<ByteBuffer> columns)
    throws InvalidRequestException
    {
        for (ByteBuffer name : columns)
        {
            if (name.remaining() > IColumn.MAX_NAME_LENGTH)
                throw new InvalidRequestException(String.format("column name is too long (%s > %s)",
                                                                name.remaining(),
                                                                IColumn.MAX_NAME_LENGTH));
            if (name.remaining() == 0)
                throw new InvalidRequestException("zero-length column name");
        }
    }

    public static void validateColumnName(ByteBuffer column)
    throws InvalidRequestException
    {
        validateColumnNames(Collections.singletonList(column));
    }

    public static void validateSlicePredicate(CFMetaData metadata, SlicePredicate predicate)
    throws InvalidRequestException
    {
        if (predicate.slice_range != null)
            validateSliceRange(metadata, predicate.slice_range);
        else
            validateColumnNames(predicate.column_names);
    }

    public static void validateSliceRange(CFMetaData metadata, SliceRange range)
    throws InvalidRequestException
    {
        validateSliceRange(metadata, range.start, range.finish, range.reversed);
    }

    public static void validateSliceRange(CFMetaData metadata, ByteBuffer start, ByteBuffer finish, boolean reversed)
    throws InvalidRequestException
    {
        AbstractType<?> comparator = metadata.getComparatorFor(null);
        Comparator<ByteBuffer> orderedComparator = reversed ? comparator.reverseComparator: comparator;
        if (start.remaining() > 0 && finish.remaining() > 0 && orderedComparator.compare(start, finish) > 0)
            throw new InvalidRequestException("Range finish must come after start in traversal order");
    }

    private static CqlResult processStatement(CQLStatement statement, ClientState clientState, List<ByteBuffer> variables, CQLExecutionContext context)
    throws  UnavailableException, InvalidRequestException, TimedOutException, SchemaDisagreementException
    {
        statement.validate(clientState);
        statement.checkAccess(clientState);
        context.clientState = clientState;
        statement = preExecutionHook.execute(statement, context);

        CqlResult result = statement.execute(clientState, variables);
        if (result == null)
        {
            result = new CqlResult();
            result.type = CqlResultType.VOID;
        }

        postExecutionHook.execute(statement, context);
        return result;
    }

    public static CqlResult process(String queryString, ClientState clientState)
    throws RecognitionException, UnavailableException, InvalidRequestException, TimedOutException, SchemaDisagreementException
    {
        logger.trace("CQL QUERY: {}", queryString);

        CQLExecutionContext context = new CQLExecutionContext();
        context.queryString = queryString;

        return processStatement(getStatement(queryString, clientState).statement, clientState, Collections.<ByteBuffer>emptyList(), context);
    }

    public static CqlResult processInternal(String query) throws UnavailableException, InvalidRequestException, TimedOutException
    {
        try
        {
            ClientState state = new ClientState(true);
            CQLStatement statement = getStatement(query, state).statement;

            statement.validate(state);
            CqlResult result = statement.execute(state, Collections.<ByteBuffer>emptyList());

            if (result == null || result.rows.isEmpty())
            {
                result = new CqlResult();
                result.type = CqlResultType.VOID;
            }

            return result;
        }
        catch (RecognitionException e)
        {
            throw new AssertionError(e);
        }
        catch (SchemaDisagreementException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static UntypedResultSet resultify(String queryString, Row row)
    {
        SelectStatement ss;
        try
        {
            ss = (SelectStatement) getStatement(queryString, null).statement;
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (RecognitionException e)
        {
            throw new RuntimeException(e);
        }

        List<CqlRow> cqlRows;
        try
        {
            cqlRows = ss.process(Collections.singletonList(row));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }

        return new UntypedResultSet(cqlRows);
    }

    public static CqlPreparedResult prepare(String queryString, ClientState clientState)
    throws RecognitionException, InvalidRequestException
    {
        logger.trace("CQL QUERY: {}", queryString);

        ParsedStatement.Prepared prepared = getStatement(queryString, clientState);
        prepared.cqlString = queryString;
        int statementId = makeStatementId(queryString);
        clientState.getCQL3Prepared().put(statementId, prepared);
        logger.trace(String.format("Stored prepared statement #%d with %d bind markers",
                                   statementId,
                                   prepared.statement.getBoundsTerms()));

        assert prepared.statement.getBoundsTerms() == prepared.boundNames.size();
        List<String> var_types = new ArrayList<String>(prepared.boundNames.size()) ;
        List<String> var_names = new ArrayList<String>(prepared.boundNames.size());
        for (CFDefinition.Name n : prepared.boundNames)
        {
            var_types.add(SelectStatement.getShortTypeName(n.type));
            var_names.add(n.name.toString());
        }

        CqlPreparedResult result = new CqlPreparedResult(statementId, prepared.boundNames.size());
        result.setVariable_types(var_types);
        result.setVariable_names(var_names);

        CQLExecutionContext context = new CQLExecutionContext();
        context.queryString = queryString;
        context.clientState = clientState;
        onPrepareHook.execute(prepared.statement, context);

        return result;
    }

    public static CqlResult processPrepared(ParsedStatement.Prepared statement, ClientState clientState, List<ByteBuffer> variables)
    throws UnavailableException, InvalidRequestException, TimedOutException, SchemaDisagreementException
    {
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.statement.getBoundsTerms() == 0)))
        {
            if (variables.size() != statement.statement.getBoundsTerms())
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.statement.getBoundsTerms(),
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        CQLExecutionContext context = new CQLExecutionContext();
        context.variables = variables;
        context.boundNames = statement.boundNames;
        context.queryString = statement.cqlString;

        return processStatement(statement.statement, clientState, variables, context);
    }

    private static final int makeStatementId(String cql)
    {
        // use the hash of the string till something better is provided
        return cql.hashCode();
    }

    private static ParsedStatement.Prepared getStatement(String queryStr, ClientState clientState) throws InvalidRequestException, RecognitionException
    {
        ParsedStatement statement = parseStatement(queryStr);

        // Set keyspace for statement that require login
        if (statement instanceof CFStatement)
            ((CFStatement)statement).prepareKeyspace(clientState);

        return statement.prepare();
    }

    public static ParsedStatement parseStatement(String queryStr) throws InvalidRequestException, RecognitionException
    {
        try
        {
            // Lexer and parser
            CharStream stream = new ANTLRStringStream(queryStr);
            CqlLexer lexer = new CqlLexer(stream);
            TokenStream tokenStream = new CommonTokenStream(lexer);
            CqlParser parser = new CqlParser(tokenStream);
    
            // Parse the query string to a statement instance
            ParsedStatement statement = parser.query();
    
            // The lexer and parser queue up any errors they may have encountered
            // along the way, if necessary, we turn them into exceptions here.
            lexer.throwLastRecognitionError();
            parser.throwLastRecognitionError();
    
            return statement;
        }
        catch (RuntimeException re)
        {
            InvalidRequestException ire = new InvalidRequestException("Failed parsing statement: [" + queryStr + "] reason: " + re.getClass().getSimpleName() + " " + re.getMessage());
            ire.initCause(re);
            throw ire;
        }
    }

}
