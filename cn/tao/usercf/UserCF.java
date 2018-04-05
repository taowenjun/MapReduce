package cn.tao.usercf;

import java.util.HashMap;
import java.util.Map;

import cn.tao.usercf.prepare.PrepareData;
import cn.tao.usercf.recommend.RecommendGoods;
import cn.tao.usercf.recommend.RecommendMatrix;
import cn.tao.usercf.similiar.SimiliarMatrix;
import cn.tao.usercf.similiar.SimiliarUser;


/**
 *@author  Tao wenjun
 *UserCF
 */

public class UserCF {
    private static String URL="hdfs://10.108.21.2:9000/recommend/UserCF/";
	public static void main(String[] args) throws Exception {
		Map<String,String> map = new HashMap<>();
		map.put("step1_input", URL+"data/");
		map.put("step1_output",URL+"step1");
		map.put("step2_input",map.get("step1_output"));
		map.put("step2_output",URL+"/step2");
		map.put("step3_input",map.get("step2_output"));
		map.put("step3_output",URL+"step3");
		map.put("step4_input",map.get("step3_output"));
		map.put("step4_output",URL+"step4");
		map.put("step5_input",map.get("step4_output"));
		map.put("step5_output",URL+"step5");
		
		if(PrepareData.run(map)==0){
			if(SimiliarMatrix.run(map)==0){
				if(SimiliarUser.run(map)==0){
					if(RecommendMatrix.run(map)==0){
						if(RecommendGoods.run(map)==0){
							System.out.println("successfully");
						}
						
					}					
				}
			}
		}
	}
}
