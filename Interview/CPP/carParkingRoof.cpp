#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
using namespace std;

long carParkingRoof(vector<long> cars, int k) {
    sort(cars.begin(), cars.end());

    long best = LLONG_MAX;
    for (int i = 0; i + k - 1 < cars.size(); i++) {
        long length = cars[i + k - 1] - cars[i] + 1;
        best = min(best, length);
    }
    return best;
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int n;
    cin >> n;

    vector<long> cars(n);
    for (int i = 0; i <n; i++) cin >> cars[i];

    int k;
    cin >> k;

    cout << carParkingRoof(cars, k) << "\n";
    return 0;
}