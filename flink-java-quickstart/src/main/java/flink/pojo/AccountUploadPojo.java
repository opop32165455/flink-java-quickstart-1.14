package flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 账号列表 临时上传
 *
 * @author zhangxuecheng4441
 * @date 2022/8/9/009 17:15
 */
@AllArgsConstructor
@Builder
@Data
@Accessors(chain = true)
@NoArgsConstructor
public class AccountUploadPojo implements Serializable, EsDoc  {
    private static final long serialVersionUID = 1817709916450829509L;
    private String uid;

    /**
     * 项目名称
     */
    private String name;
    /**
     * 导入标记
     */
    private String tags;


    @Override
    public String getDocId() {
        return uid;
    }

    @Override
    public String getRouting() {
        return uid;
    }
}
