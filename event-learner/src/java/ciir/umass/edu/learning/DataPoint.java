/*===============================================================================
 * Copyright (c) 2010-2012 University of Massachusetts.  All Rights Reserved.
 *
 * Use of the RankLib package is subject to the terms of the software license set 
 * forth in the LICENSE file included with this software, and also available at
 * http://people.cs.umass.edu/~vdang/ranklib_license.html
 *===============================================================================
 */

package ciir.umass.edu.learning;

/**
 * @author vdang
 *         <p>
 *         This class implements objects to be ranked. In the context of Information retrieval, each instance is a query-url pair represented by a n-dimentional feature vector.
 *         It should be general enough for other ranking applications as well (not limited to just IR I hope).
 */
public class DataPoint {

    //attributes
    public int label = 0;//[ground truth] the real label of the data point (e.g. its degree of relevance according to the relevance judgment)
    public String id = "";//id of this datapoint (e.g. query-id)

    public String description = "";


    protected DataPoint() {
    }

    public DataPoint(DataPoint dp) {
        label = dp.label;
        id = dp.id;
        description = dp.description;

    }

    public DataPoint(String id, int label) {
        this.label = label;
        this.id = id;
    }


    public String getID() {
        return id;
    }

    public void setID(String id) {
        this.id = id;
    }

    public float getLabel() {
        return label;
    }

    public void setLabel(int label) {
        this.label = label;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
