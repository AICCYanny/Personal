#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
using namespace std;

long maxInversions(vector<int> arr) {
    int n = arr.size();
    long count = 0;

    for (int j = 0; j < n; j++) {
        long leftGreater = 0;
        long rightSmaller = 0;

        for (int i = 0; i < j; i++)
            if (arr[i] > arr[j])
                leftGreater++;

        for (int k = j + 1; k < n; k++)
            if (arr[k] < arr[j])
                rightSmaller++;

        count += leftGreater * rightSmaller;
    }

    return count;
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int n;
    cin >> n;

    vector<int> arr(n);
    for (int i = 0; i < n; i++) cin >> arr[i];

    cout << maxInversions(arr) << "\n";
    return 0;
}