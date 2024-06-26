package questions

import "slices"

func lengthOfLIS(nums []int) int {
	list := make([]int, len(nums))
	for i := range list {
		list[i] = 1
	}

	for i := len(nums); i >= 0; i-- {
		for j := i + 1; j < len(nums); j++ {
			if nums[i] < nums[j] {
				list[i] = max(list[i], 1+list[j])
			}
		}
	}
	return slices.Max(list)
}

func lengthOfLISRecursive(nums []int) int {
	result := 0
	mem := make(map[int]int, len(nums))
	var dfs func(i int) int
	dfs = func(i int) int {
		if i >= len(nums) {
			return 0
		}
		if _, ok := mem[i]; ok {
			return mem[i]
		}

		temp := 1
		for j := i + 1; j < len(nums); j++ {
			if nums[i] < nums[j] {

				temp = max(temp, 1+dfs(j))
			}
		}
		mem[i] = temp

		return temp
	}

	for index := range nums {
		result = max(result, dfs(index))
	}

	return result
}
