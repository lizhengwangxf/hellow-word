"""计算器视图 - 命令行交互界面"""

from cal.calculator import Calculator


def run():
    """运行计算器交互界面"""
    calc = Calculator()
    print("=" * 40)
    print("       简单计算器 - 四则混合运算")
    print("=" * 40)
    print("支持运算符: +  -  *  /  (  )")
    print("输入表达式进行计算，输入 q 退出")
    print("=" * 40)

    while True:
        try:
            user_input = input("\n请输入表达式: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n再见！")
            break

        if not user_input:
            continue
        if user_input.lower() == "q":
            print("再见！")
            break

        try:
            result = calc.evaluate(user_input)
            # 如果结果是整数，显示为整数
            if isinstance(result, float) and result.is_integer():
                result = int(result)
            print(f"结果: {result}")
        except ValueError as e:
            print(f"错误: {e}")
        except Exception as e:
            print(f"未知错误: {e}")


if __name__ == "__main__":
    run()
