package questions

import "github.com/hi-rustin/lg/src/utils"

func diameterOfBinaryTree(root *utils.TreeNode) int {
	res := 0

	var dfs func(node *utils.TreeNode) int
	dfs = func(node *utils.TreeNode) int {
		if node == nil {
			return -1
		}
		left := dfs(node.Left)
		right := dfs(node.Right)
		res = max(res, 2+left+right)
		return 1 + max(left, right)
	}

	dfs(root)
	return res
}
