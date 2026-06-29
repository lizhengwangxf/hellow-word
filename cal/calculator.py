"""简单计算器 - 支持四则混合运算"""


class Calculator:
    """计算器类，支持加减乘除四则混合运算"""

    def add(self, a, b):
        """加法"""
        return a + b

    def subtract(self, a, b):
        """减法"""
        return a - b

    def multiply(self, a, b):
        """乘法"""
        return a * b

    def divide(self, a, b):
        """除法"""
        if b == 0:
            raise ValueError("除数不能为零")
        return a / b

    def evaluate(self, expression):
        """
        计算四则混合运算表达式
        支持运算符: +, -, *, /
        支持括号改变优先级
        """
        # 去除空格
        expression = expression.replace(" ", "")
        if not expression:
            raise ValueError("表达式不能为空")

        # 使用递归下降解析器处理表达式
        self._expr = expression
        self._pos = 0
        result = self._parse_expression()
        if self._pos < len(self._expr):
            raise ValueError(f"无效字符: {self._expr[self._pos]}")
        return result

    def _parse_expression(self):
        """解析加减法（最低优先级）"""
        result = self._parse_term()
        while self._pos < len(self._expr) and self._expr[self._pos] in "+-":
            op = self._expr[self._pos]
            self._pos += 1
            right = self._parse_term()
            if op == "+":
                result = result + right
            else:
                result = result - right
        return result

    def _parse_term(self):
        """解析乘除法（较高优先级）"""
        result = self._parse_factor()
        while self._pos < len(self._expr) and self._expr[self._pos] in "*/":
            op = self._expr[self._pos]
            self._pos += 1
            right = self._parse_factor()
            if op == "*":
                result = result * right
            else:
                if right == 0:
                    raise ValueError("除数不能为零")
                result = result / right
        return result

    def _parse_factor(self):
        """解析数字和括号"""
        if self._pos < len(self._expr) and self._expr[self._pos] == "(":
            self._pos += 1  # 跳过 '('
            result = self._parse_expression()
            if self._pos >= len(self._expr) or self._expr[self._pos] != ")":
                raise ValueError("缺少右括号")
            self._pos += 1  # 跳过 ')'
            return result

        # 解析数字（支持负数和小数）
        start = self._pos
        if self._pos < len(self._expr) and self._expr[self._pos] == "-":
            self._pos += 1
        while self._pos < len(self._expr) and (self._expr[self._pos].isdigit() or self._expr[self._pos] == "."):
            self._pos += 1

        if start == self._pos:
            raise ValueError(f"期望数字，得到: {self._expr[self._pos] if self._pos < len(self._expr) else '结束'}")

        return float(self._expr[start:self._pos])
