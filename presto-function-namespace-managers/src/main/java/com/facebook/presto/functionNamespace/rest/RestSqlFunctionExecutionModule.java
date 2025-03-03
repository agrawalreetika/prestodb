/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.functionNamespace.rest;

import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutionModule;
import com.facebook.presto.spi.function.SqlFunctionExecutor;
import com.google.inject.Binder;
import com.google.inject.Scopes;

public class RestSqlFunctionExecutionModule
        extends SqlFunctionExecutionModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(SqlFunctionExecutor.class).to(RestSqlFunctionExecutor.class).in(Scopes.SINGLETON);
    }
}
