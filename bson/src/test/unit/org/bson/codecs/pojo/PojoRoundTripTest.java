/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.BsonDocument;
import org.bson.codecs.SimpleEnum;
import org.bson.codecs.pojo.entities.AbstractInterfaceModel;
import org.bson.codecs.pojo.entities.CollectionNestedPojoModel;
import org.bson.codecs.pojo.entities.CollectionSpecificReturnTypeCreatorModel;
import org.bson.codecs.pojo.entities.CollectionSpecificReturnTypeModel;
import org.bson.codecs.pojo.entities.ConcreteAndNestedAbstractInterfaceModel;
import org.bson.codecs.pojo.entities.ConcreteCollectionsModel;
import org.bson.codecs.pojo.entities.ConcreteInterfaceGenericModel;
import org.bson.codecs.pojo.entities.ConcreteStandAloneAbstractInterfaceModel;
import org.bson.codecs.pojo.entities.ContainsAlternativeMapAndCollectionModel;
import org.bson.codecs.pojo.entities.ConventionModel;
import org.bson.codecs.pojo.entities.DuplicateAnnotationAllowedModel;
import org.bson.codecs.pojo.entities.FieldAndPropertyTypeMismatchModel;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.GenericTreeModel;
import org.bson.codecs.pojo.entities.InterfaceBasedModel;
import org.bson.codecs.pojo.entities.InterfaceModelImpl;
import org.bson.codecs.pojo.entities.InterfaceUpperBoundsModelAbstractImpl;
import org.bson.codecs.pojo.entities.InterfaceWithDefaultMethodModelImpl;
import org.bson.codecs.pojo.entities.InterfaceWithOverrideDefaultMethodModelImpl;
import org.bson.codecs.pojo.entities.ListGenericExtendedModel;
import org.bson.codecs.pojo.entities.ListListGenericExtendedModel;
import org.bson.codecs.pojo.entities.ListMapGenericExtendedModel;
import org.bson.codecs.pojo.entities.MapGenericExtendedModel;
import org.bson.codecs.pojo.entities.MapListGenericExtendedModel;
import org.bson.codecs.pojo.entities.MapMapGenericExtendedModel;
import org.bson.codecs.pojo.entities.MultipleBoundsModel;
import org.bson.codecs.pojo.entities.MultipleLevelGenericModel;
import org.bson.codecs.pojo.entities.NestedFieldReusingClassTypeParameter;
import org.bson.codecs.pojo.entities.NestedGenericHolderFieldWithMultipleTypeParamsModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderMapModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderSimpleGenericsModel;
import org.bson.codecs.pojo.entities.NestedGenericTreeModel;
import org.bson.codecs.pojo.entities.NestedMultipleLevelGenericModel;
import org.bson.codecs.pojo.entities.NestedReusedGenericsModel;
import org.bson.codecs.pojo.entities.NestedSelfReferentialGenericHolderModel;
import org.bson.codecs.pojo.entities.NestedSelfReferentialGenericModel;
import org.bson.codecs.pojo.entities.NestedSimpleIdModel;
import org.bson.codecs.pojo.entities.PrimitivesModel;
import org.bson.codecs.pojo.entities.PropertyReusingClassTypeParameter;
import org.bson.codecs.pojo.entities.PropertySelectionModel;
import org.bson.codecs.pojo.entities.PropertyWithMultipleTypeParamsModel;
import org.bson.codecs.pojo.entities.ReusedGenericsModel;
import org.bson.codecs.pojo.entities.SelfReferentialGenericModel;
import org.bson.codecs.pojo.entities.ShapeHolderCircleModel;
import org.bson.codecs.pojo.entities.ShapeHolderModel;
import org.bson.codecs.pojo.entities.ShapeModelAbstract;
import org.bson.codecs.pojo.entities.ShapeModelCircle;
import org.bson.codecs.pojo.entities.ShapeModelRectangle;
import org.bson.codecs.pojo.entities.SimpleEnumModel;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleIdImmutableModel;
import org.bson.codecs.pojo.entities.SimpleIdModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.SimpleNestedPojoModel;
import org.bson.codecs.pojo.entities.SimpleWithStaticModel;
import org.bson.codecs.pojo.entities.TreeWithIdModel;
import org.bson.codecs.pojo.entities.UpperBoundsConcreteModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationBsonPropertyIdModel;
import org.bson.codecs.pojo.entities.conventions.BsonExtraElementsMapModel;
import org.bson.codecs.pojo.entities.conventions.BsonExtraElementsModel;
import org.bson.codecs.pojo.entities.conventions.BsonIgnoreDuplicatePropertyMultipleTypes;
import org.bson.codecs.pojo.entities.conventions.BsonIgnoreInvalidMapModel;
import org.bson.codecs.pojo.entities.conventions.BsonIgnoreSyntheticProperty;
import org.bson.codecs.pojo.entities.conventions.BsonRepresentationModel;
import org.bson.codecs.pojo.entities.conventions.CollectionDiscriminatorAbstractClassesModel;
import org.bson.codecs.pojo.entities.conventions.CollectionDiscriminatorInterfacesModel;
import org.bson.codecs.pojo.entities.conventions.CreatorAllFinalFieldsModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorIdModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorLegacyBsonPropertyModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorRenameModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInSuperClassModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInSuperClassModelImpl;
import org.bson.codecs.pojo.entities.conventions.CreatorMethodModel;
import org.bson.codecs.pojo.entities.conventions.CreatorNoArgsConstructorModel;
import org.bson.codecs.pojo.entities.conventions.CreatorNoArgsMethodModel;
import org.bson.codecs.pojo.entities.conventions.InterfaceModel;
import org.bson.codecs.pojo.entities.conventions.InterfaceModelImplA;
import org.bson.codecs.pojo.entities.conventions.InterfaceModelImplB;
import org.bson.codecs.pojo.entities.conventions.Subclass1Model;
import org.bson.codecs.pojo.entities.conventions.Subclass2Model;
import org.bson.codecs.pojo.entities.conventions.SuperClassModel;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
public final class PojoRoundTripTest extends PojoTestCase {

    private final String name;
    private final Object model;
    private final PojoCodecProvider.Builder builder;
    private final String json;

    public PojoRoundTripTest(final String name, final Object model, final String json, final PojoCodecProvider.Builder builder) {
        this.name = name;
        this.model = model;
        this.json = json;
        this.builder = builder;
    }

    @Test
    public void test() {
        roundTrip(builder, model, json);
        threadedRoundTrip(builder, model, json);
    }

    private static List<TestData> testCases() {
        List<TestData> data = new ArrayList<>();

        data.add(new TestData("Self referential", getNestedSelfReferentialGenericHolderModel(),
                getPojoCodecProviderBuilder(NestedSelfReferentialGenericHolderModel.class, NestedSelfReferentialGenericModel.class,
                        SelfReferentialGenericModel.class),
                "{'nested': { 't': true } }"));

        return data;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();

        for (TestData testData : testCases()) {
            data.add(new Object[]{format("%s", testData.getName()), testData.getModel(), testData.getJson(), testData.getBuilder()});
//            data.add(new Object[]{format("%s [Auto]", testData.getName()), testData.getModel(), testData.getJson(), AUTOMATIC_BUILDER});
//            data.add(new Object[]{format("%s [Package]", testData.getName()), testData.getModel(), testData.getJson(), PACKAGE_BUILDER});
        }
        return data;
    }

    private static final PojoCodecProvider.Builder AUTOMATIC_BUILDER = PojoCodecProvider.builder().automatic(true);
    private static final PojoCodecProvider.Builder PACKAGE_BUILDER = PojoCodecProvider.builder().register("org.bson.codecs.pojo.entities",
            "org.bson.codecs.pojo.entities.conventions");

    private static class TestData {
        private final String name;
        private final Object model;
        private final PojoCodecProvider.Builder builder;
        private final String json;

        TestData(final String name, final Object model, final PojoCodecProvider.Builder builder, final String json) {
            this.name = name;
            this.model = model;
            this.builder = builder;
            this.json = json;
        }

        public String getName() {
            return name;
        }

        public Object getModel() {
            return model;
        }

        public PojoCodecProvider.Builder getBuilder() {
            return builder;
        }

        public String getJson() {
            return json;
        }
    }


}
