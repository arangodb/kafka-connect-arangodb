package com.arangodb.kafka.utils;

import com.arangodb.ArangoCollection;
import com.arangodb.kafka.deployment.KafkaConnectDeployment;
import com.arangodb.kafka.deployment.KafkaConnectOperations;
import com.arangodb.kafka.target.Connector;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.TargetHolder;
import com.arangodb.kafka.target.TestTarget;
import org.junit.jupiter.api.extension.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TargetProvider implements TestTemplateInvocationContextProvider {
    private static final KafkaConnectOperations connectClient = KafkaConnectDeployment.getInstance().client();

    private static TestTarget instantiate(Class<? extends TestTarget> clazz, String testNamePrefix) {
        try {
            Constructor<? extends TestTarget> constructor = clazz.getConstructor(String.class);
            return constructor.newInstance(testNamePrefix + clazz.getSimpleName());
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        String testNamePrefix = context.getRequiredTestClass().getSimpleName() + "-" + context.getRequiredTestMethod().getName() + "-";
        Class<? extends TestTarget>[] value = context.getRequiredTestMethod().getAnnotation(KafkaTest.class).value();
        Class<? extends TargetHolder> targetGroup = context.getRequiredTestMethod().getAnnotation(KafkaTest.class).group();
        Stream<Class<? extends TestTarget>> targets = value.length > 0 ? Arrays.stream(value) :
                Arrays.stream(targetGroup.getEnumConstants()).map(TargetHolder::getClazz);
        return targets
                .map(it -> new TestTemplateInvocationContext() {

                    private final TestTarget target = instantiate(it, testNamePrefix);

                    @Override
                    public String getDisplayName(int invocationIndex) {
                        return it.getSimpleName();
                    }

                    @Override
                    public List<Extension> getAdditionalExtensions() {
                        return Arrays.asList(
                                new ParamSupplier<>(ArangoCollection.class, target::getCollection),
                                new ParamSupplier<>(Connector.class, () -> target),
                                new ParamSupplier<>(Producer.class, () -> target),
                                new ParamSupplier<>(KafkaConnectOperations.class, () -> connectClient),
                                (BeforeTestExecutionCallback) extensionContext -> target.init(),
                                (AfterTestExecutionCallback) extensionContext -> target.close()
                        );
                    }
                });
    }

    static final class ParamSupplier<T> implements ParameterResolver {
        private final Class<T> clazz;
        private final Supplier<T> supplier;

        ParamSupplier(Class<T> clazz, Supplier<T> supplier) {
            this.clazz = clazz;
            this.supplier = supplier;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return parameterContext.getParameter().getType() == clazz;
        }

        @Override
        public T resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return supplier.get();
        }
    }
}
