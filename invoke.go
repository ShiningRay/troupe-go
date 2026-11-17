package troupe

import (
	"context"
	"encoding/json"
	"fmt"
)

// Invoke 泛型类型安全调用（M2 "类型安全 Agent" 的 Go 形态；troupe.cr 宏生成
// 方法的等价物）：Args/Res 编译期确定，结果经 JSON 边界反序列化；
// 方法名仍走白名单（未知方法运行时拒绝——显式注册纪律优先于反射魔法）。
//
//	res, err := troupe.Invoke[AddArgs, int](ctx, stage, id, "add", AddArgs{Item: "x"})
func Invoke[Args, Res any](ctx context.Context, st *Stage, id ActorID, method string, args Args, opts ...CallOption) (Res, error) {
	var zero Res
	f, err := st.Call(ctx, id, method, args, opts...)
	if err != nil {
		return zero, err
	}
	raw, err := f.Await(ctx)
	if err != nil {
		return zero, err
	}
	var res Res
	if err := json.Unmarshal(raw, &res); err != nil {
		return zero, fmt.Errorf("结果反序列化失败（%s）：%w", method, err)
	}
	return res, nil
}
