/**  
 * All rights Reserved, Designed By grq
 * @Title:  Demo.java   
 * @Package spark.study.bookb   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: grq  
 * @date:   2019年1月16日 下午7:54:05   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.bookb;

import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.junit.Test;

import com.sun.tools.classfile.Opcode.Set;

import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;

/**
 * @author MI
 *
 */
public class Demo {
	public static void main(String[] args) {
		// java 创建一个向量
		// 创建稠密向量<1.0,2.0,3.0>; Vectors.dense接收一串值或一个数组
		Vector dense = Vectors.dense(1.0, 2.0, 3.0);// [1.0,2.0,3.0]
		System.out.println(dense);
		Vector dense2 = Vectors.dense(new double[] { 1.0, 2.0, 3.0 });

		// 创建稀疏向量<1.0,0.0,2.0,0.0> 向量维度(此处是4)，非零位置和对应的值
		Vector sparse = Vectors.sparse(4, new int[] { 0, 2 }, new double[] { 1.0, 2.0 });
		System.out.println(sparse);// (4,[0,2],[1.0,2.0])

	}

	@Test
	public void testLabel() throws Exception {
		LabeledPoint labeledPoint = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

		// 创建稀疏向量
		LabeledPoint labeledPoint2 = new LabeledPoint(1.0, Vectors.sparse(4, new int[] { 0, 2 }, new double[] { 1.0, 2.0 }));
		System.out.println(labeledPoint);// (1.0,[1.0,0.0,3.0])
		System.out.println(labeledPoint2);// (1.0,(4,[0,2],[1.0,2.0]))
	}

	/**
	 * 矩阵
	 */
	@Test
	public void testmatrix() {
		Matrix matrix = Matrices.dense(3, 2, new double[] { 1.0, 3.0, 5.0, 2.0, 4.0, 6.0 });
		System.out.println(matrix);

		// 1.0 2.0
		// 3.0 4.0
		// 5.0 6.0
		Matrix matrix2 = Matrices.sparse(3, 2, new int[] { 0, 1, 3 }, new int[] { 0, 2, 1 }, new double[] { 9, 6, 8 });
		System.out.println(matrix2);
		// 3 x 2 CSCMatrix
		// (0,0) 9.0
		// (2,1) 6.0
		// (1,1) 8.0
		// 被创建为稀疏矩阵（（9.0,0.0）,（0.0，8.0）, （0.0，6.0））。
	}

}
