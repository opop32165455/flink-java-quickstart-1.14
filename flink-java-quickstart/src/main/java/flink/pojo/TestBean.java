package flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.omg.PortableInterceptor.INACTIVE;

/**
 * @author zhangxuecheng
 * @package flink.pojo
 * @className TestBean
 * @description test bean
 * @date 2023/7/20 13:58
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TestBean implements UniqueBean {

    String id;
    Integer number;
    String desc;

    @Override
    public String uniqueKey() {
        return id;
    }
}