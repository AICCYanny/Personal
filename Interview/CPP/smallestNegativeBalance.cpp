#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
using namespace std;

vector<string> smallestNegativeBalance(vector<vector<string>> debts) {
    unordered_map<string, long long> balance;

    for (const auto& d : debts) {
        const string& borrower = d[0];
        const string& lender = d[1];
        long long amount = stoll(d[2]);

        balance[borrower] -= amount;
        balance[lender] += amount;
    }

    long long minBalance = 0;
    bool hasNegative = false;

    for (const auto& p : balance) {
        if (p.second < minBalance) {
            minBalance = p.second;
            hasNegative = true;
        }
    }

    if (!hasNegative) {
        return {"Nobody has a negative balance"};
    }

    vector<string> result;
    for (const auto& p : balance) {
        if (p.second == minBalance) {
            result.push_back(p.first);
        }
    }

    sort(result.begin(), result.end());
    return result;
}
int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int n;
    cin >> n;

    int cols;
    cin >> cols;

    vector<vector<string>> debts(n, vector<string>(3));

    for (int i = 0; i < n; i++) {
        cin >> debts[i][0] >> debts[i][1] >> debts[i][2];
    }

    vector<string> result = smallestNegativeBalance(debts);

    for (auto &name : result) cout << name << "\n";

    return 0;
}