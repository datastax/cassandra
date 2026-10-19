/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * Test for any estimated histogram that uses DSE compatible boundaries. This needs to be a separate test class because
 * the property value is inlined as static final in the EstimatedHistogram class.
 */
public class DSEEstimatedHistogramTest
{
    @BeforeClass
    public static void setup()
    {
        CassandraRelevantProperties.USE_DSE_COMPATIBLE_HISTOGRAM_BOUNDARIES.setBoolean(true);
    }

    @Test
    public void testDSEBoundaries()
    {
            // these boundaries were computed in DSE, in HistogramSnapshotTest::boundaries
            long[] dseBoundaries = new long[]{ 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 17, 20, 24, 29, 35, 42, 50, 60, 72, 86, 103, 124,
                149, 179, 215, 258, 310, 372, 446, 535, 642, 770, 924, 1109, 1331, 1597, 1916, 2299, 2759, 3311, 3973, 4768,
                5722, 6866, 8239, 9887, 11864, 14237, 17084, 20501, 24601, 29521, 35425, 42510, 51012, 61214, 73457, 88148,
                105778, 126934, 152321, 182785, 219342, 263210, 315852, 379022, 454826, 545791, 654949, 785939, 943127,
                1131752, 1358102, 1629722, 1955666, 2346799, 2816159, 3379391, 4055269, 4866323, 5839588, 7007506, 8409007,
                10090808, 12108970, 14530764, 17436917, 20924300, 25109160, 30130992, 36157190, 43388628, 52066354, 62479625,
                74975550, 89970660, 107964792, 129557750, 155469300, 186563160, 223875792, 268650950, 322381140, 386857368,
                464228842, 557074610, 668489532, 802187438, 962624926, 1155149911, 1386179893, 1663415872, 1996099046,
                2395318855L, 2874382626L, 3449259151L, 4139110981L, 4966933177L, 5960319812L, 7152383774L, 8582860529L,
                10299432635L, 12359319162L, 14831182994L, 17797419593L, 21356903512L, 25628284214L, 30753941057L,
                36904729268L, 44285675122L, 53142810146L, 63771372175L, 76525646610L, 91830775932L, 110196931118L,
                132236317342L, 158683580810L, 190420296972L, 228504356366L, 274205227639L, 329046273167L, 394855527800L,
                473826633360L, 568591960032L, 682310352038L, 818772422446L, 982526906935L, 1179032288322L, 1414838745986L,
                1697806495183L, 2037367794220L, 2444841353064L, 2933809623677L, 3520571548412L, 4224685858094L,
                5069623029713L, 6083547635656L, 7300257162787L, 8760308595344L, 10512370314413L, 12614844377296L,
                15137813252755L, 18165375903306L, 21798451083967L, 26158141300760L, 31389769560912L, 37667723473094L,
                45201268167713L, 54241521801256L, 65089826161507L, 78107791393808L, 93729349672570L, 112475219607084L,
                134970263528501L, 161964316234201L, 194357179481041L, 233228615377249L, 279874338452699L, 335849206143239L,
                403019047371887L, 483622856846264L, 580347428215517L, 696416913858620L, 835700296630344L, 1002840355956413L,
                1203408427147696L, 1444090112577235L, 1732908135092682L, 2079489762111218L, 2495387714533462L,
                2994465257440155L, 3593358308928186L, 4312029970713823L, 5174435964856587L, 6209323157827904L,
                7451187789393485L, 8941425347272182L, 10729710416726618L, 12875652500071942L, 15450783000086330L,
                18540939600103596L, 22249127520124316L, 26698953024149180L, 32038743628979016L, 38446492354774816L,
                46135790825729776L, 55362948990875728L, 66435538789050872L, 79722646546861040L, 95667175856233248L,
                114800611027479888L, 137760733232975856L, 165312879879571008L, 198375455855485216L, 238050547026582240L,
                285660656431898688L, 342792787718278400L, 411351345261934080L, 493621614314320896L, 592345937177185024L,
                710815124612621952L, 852978149535146368L, 1023573779442175616L, 1228288535330610688L, 1473946242396732672L,
                1768735490876079104L, 2122482589051294720L, 2546979106861553664L, 3056374928233864192L, 3667649913880636928L,
                4401179896656763904L, 5281415875988116480L, 6337699051185739776L, 7605238861422887936L, 9126286633707464704L };

            // the code below is O(n^2) so that we don't need to assume that boundaries are independent
            // of the histogram size; this is not a problem since the number of boundaries is small
            for (int size = 1; size <= dseBoundaries.length; size++)
            {
                EstimatedHistogram histogram = new EstimatedHistogram(size);
                // compute subarray of dseBoundaries of size `size`
                long[] subarray = new long[size];
                System.arraycopy(dseBoundaries, 0, subarray, 0, size);
                Assert.assertArrayEquals(subarray, histogram.getBucketOffsets());
            }
    }
}
