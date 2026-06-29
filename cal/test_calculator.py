"""计算器测试代码"""

import pytest
from cal.calculator import Calculator


class TestCalculatorBasic:
    """基本四则运算测试"""

    def setup_method(self):
        self.calc = Calculator()

    # 加法测试
    def test_add_positive(self):
        assert self.calc.add(2, 3) == 5

    def test_add_negative(self):
        assert self.calc.add(-1, -2) == -3

    def test_add_zero(self):
        assert self.calc.add(0, 5) == 5

    def test_add_float(self):
        assert self.calc.add(1.5, 2.5) == 4.0

    # 减法测试
    def test_subtract_positive(self):
        assert self.calc.subtract(5, 3) == 2

    def test_subtract_negative_result(self):
        assert self.calc.subtract(3, 5) == -2

    def test_subtract_zero(self):
        assert self.calc.subtract(5, 0) == 5

    # 乘法测试
    def test_multiply_positive(self):
        assert self.calc.multiply(3, 4) == 12

    def test_multiply_by_zero(self):
        assert self.calc.multiply(5, 0) == 0

    def test_multiply_negative(self):
        assert self.calc.multiply(-2, 3) == -6

    # 除法测试
    def test_divide_normal(self):
        assert self.calc.divide(10, 2) == 5

    def test_divide_float_result(self):
        assert self.calc.divide(7, 2) == 3.5

    def test_divide_by_zero(self):
        with pytest.raises(ValueError, match="除数不能为零"):
            self.calc.divide(5, 0)


class TestCalculatorExpression:
    """混合运算表达式测试"""

    def setup_method(self):
        self.calc = Calculator()

    # 基本四则运算
    def test_simple_add(self):
        assert self.calc.evaluate("1+2") == 3

    def test_simple_subtract(self):
        assert self.calc.evaluate("5-3") == 2

    def test_simple_multiply(self):
        assert self.calc.evaluate("3*4") == 12

    def test_simple_divide(self):
        assert self.calc.evaluate("10/2") == 5

    # 混合运算 - 优先级
    def test_add_and_multiply(self):
        assert self.calc.evaluate("2+3*4") == 14

    def test_multiply_and_add(self):
        assert self.calc.evaluate("3*4+2") == 14

    def test_add_and_divide(self):
        assert self.calc.evaluate("6+8/2") == 10

    def test_subtract_and_multiply(self):
        assert self.calc.evaluate("10-2*3") == 4

    # 括号运算
    def test_parentheses_add_then_multiply(self):
        assert self.calc.evaluate("(2+3)*4") == 20

    def test_parentheses_change_priority(self):
        assert self.calc.evaluate("2*(3+4)") == 14

    def test_nested_parentheses(self):
        assert self.calc.evaluate("((2+3)*4)") == 20

    def test_parentheses_with_divide(self):
        assert self.calc.evaluate("(8+2)/5") == 2

    # 复杂混合运算
    def test_complex_expression_1(self):
        assert self.calc.evaluate("2+3*4-6/2") == 11

    def test_complex_expression_2(self):
        assert self.calc.evaluate("(1+2)*(3+4)") == 21

    def test_complex_expression_3(self):
        assert self.calc.evaluate("10/(2+3)*4") == 8.0

    def test_complex_expression_4(self):
        assert self.calc.evaluate("2*3+4*5-6/2") == 23

    # 带空格
    def test_with_spaces(self):
        assert self.calc.evaluate("2 + 3 * 4") == 14

    # 小数运算
    def test_float_expression(self):
        assert self.calc.evaluate("1.5+2.5") == 4.0

    def test_float_multiply(self):
        assert self.calc.evaluate("0.5*6") == 3.0

    # 错误处理
    def test_empty_expression(self):
        with pytest.raises(ValueError, match="表达式不能为空"):
            self.calc.evaluate("")

    def test_divide_by_zero_in_expression(self):
        with pytest.raises(ValueError, match="除数不能为零"):
            self.calc.evaluate("5/0")

    def test_missing_right_parenthesis(self):
        with pytest.raises(ValueError, match="缺少右括号"):
            self.calc.evaluate("(2+3")

    def test_invalid_character(self):
        with pytest.raises(ValueError):
            self.calc.evaluate("2%3")
