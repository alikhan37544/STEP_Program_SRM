/*
problem statement is to create a binary tree.
Implement memory management, the best practices
implement an iterator

push back / push front for input and delete

push back / push front for traversal

*/

// First let's create a binary tree node structure, implementing memory management and best practices.

#include <iostream>
#include <memory>
#include <vector>
#include <queue>
#include <stack>
#include <sstream>
#include <functional>

// Binary tree node
template <typename T>
class BinaryTreeNode {
public:
    T data;
    std::unique_ptr<BinaryTreeNode<T>> left;
    std::unique_ptr<BinaryTreeNode<T>> right;
    BinaryTreeNode(T value) : data(value), left(nullptr), right(nullptr) {}
};

// Doubly linked list node for level order pointer list
template <typename T>
struct LevelOrderPointerListNode {
    T* dataPtr;
    LevelOrderPointerListNode* prev;
    LevelOrderPointerListNode* next;
    LevelOrderPointerListNode(T* ptr) : dataPtr(ptr), prev(nullptr), next(nullptr) {}
};

// Doubly linked list for level order pointer list
template <typename T>
class LevelOrderPointerList {
    LevelOrderPointerListNode<T>* head;
    LevelOrderPointerListNode<T>* tail;
    size_t count;
public:
    LevelOrderPointerList() : head(nullptr), tail(nullptr), count(0) {}
    ~LevelOrderPointerList() { clear(); }

    void push_back(T* ptr) {
        auto* node = new LevelOrderPointerListNode<T>(ptr);
        if (!tail) {
            head = tail = node;
        } else {
            tail->next = node;
            node->prev = tail;
            tail = node;
        }
        ++count;
    }

    void clear() {
        auto* curr = head;
        while (curr) {
            auto* tmp = curr;
            curr = curr->next;
            delete tmp;
        }
        head = tail = nullptr;
        count = 0;
    }

    LevelOrderPointerListNode<T>* getHead() const { return head; }
    LevelOrderPointerListNode<T>* getTail() const { return tail; }
    size_t size() const { return count; }

    // Bidirectional iterator
    class Iterator {
        LevelOrderPointerListNode<T>* node;
    public:
        Iterator(LevelOrderPointerListNode<T>* n) : node(n) {}
        Iterator& operator++() { if (node) node = node->next; return *this; }
        Iterator& operator--() { if (node) node = node->prev; return *this; }
        T* operator*() const { return node ? node->dataPtr : nullptr; }
        bool operator!=(const Iterator& other) const { return node != other.node; }
        bool operator==(const Iterator& other) const { return node == other.node; }
        bool valid() const { return node != nullptr; }
    };

    Iterator begin() const { return Iterator(head); }
    Iterator end() const { return Iterator(nullptr); }
    Iterator rbegin() const { return Iterator(tail); }
};

template <typename T>
class BinaryTree {
private:
    std::unique_ptr<BinaryTreeNode<T>> root;

    void insertLevelOrder(T value) {
        if (!root) {
            root = std::make_unique<BinaryTreeNode<T>>(value);
            return;
        }
        std::queue<BinaryTreeNode<T>*> q;
        q.push(root.get());
        while (!q.empty()) {
            BinaryTreeNode<T>* node = q.front();
            q.pop();
            if (!node->left) {
                node->left = std::make_unique<BinaryTreeNode<T>>(value);
                return;
            } else {
                q.push(node->left.get());
            }
            if (!node->right) {
                node->right = std::make_unique<BinaryTreeNode<T>>(value);
                return;
            } else {
                q.push(node->right.get());
            }
        }
    }

    void inorderTraversal(const std::unique_ptr<BinaryTreeNode<T>>& node, std::vector<T>& result) const {
        if (node) {
            inorderTraversal(node->left, result);
            result.push_back(node->data);
            inorderTraversal(node->right, result);
        }
    }

    void preorderTraversal(const std::unique_ptr<BinaryTreeNode<T>>& node, std::vector<T>& result) const {
        if (node) {
            result.push_back(node->data);
            preorderTraversal(node->left, result);
            preorderTraversal(node->right, result);
        }
    }

    void postorderTraversal(const std::unique_ptr<BinaryTreeNode<T>>& node, std::vector<T>& result) const {
        if (node) {
            postorderTraversal(node->left, result);
            postorderTraversal(node->right, result);
            result.push_back(node->data);
        }
    }

    void levelOrderTraversal(const std::unique_ptr<BinaryTreeNode<T>>& node, std::vector<T>& result) const {
        if (!node) return;
        std::queue<BinaryTreeNode<T>*> q;
        q.push(node.get());
        while (!q.empty()) {
            BinaryTreeNode<T>* current = q.front();
            q.pop();
            result.push_back(current->data);
            if (current->left) q.push(current->left.get());
            if (current->right) q.push(current->right.get());
        }
    }

public:
    BinaryTree() : root(nullptr) {}

    void insert(T value) {
        insertLevelOrder(value);
    }

    std::vector<T> inorder() const {
        std::vector<T> result;
        inorderTraversal(root, result);
        return result;
    }

    std::vector<T> preorder() const {
        std::vector<T> result;
        preorderTraversal(root, result);
        return result;
    }

    std::vector<T> postorder() const {
        std::vector<T> result;
        postorderTraversal(root, result);
        return result;
    }

    std::vector<T> levelOrder() const {
        std::vector<T> result;
        levelOrderTraversal(root, result);
        return result;
    }

    bool isEmpty() const {
        return !root;
    }

    void clear() {
        root.reset();
    }

    size_t size() const {
        std::function<size_t(const std::unique_ptr<BinaryTreeNode<T>>&)> count
        = [&](const std::unique_ptr<BinaryTreeNode<T>>& node) {
            if (!node) return size_t(0);
            return size_t(1) + count(node->left) + count(node->right);
        };
        return count(root);
    }

    std::string toString() const {
        std::ostringstream oss;
        std::function<void(const std::unique_ptr<BinaryTreeNode<T>>&)> serialize
        = [&](const std::unique_ptr<BinaryTreeNode<T>>& node) {
            if (!node) {
                oss << "null ";
                return;
            }
            oss << node->data << " ";
            serialize(node->left);
            serialize(node->right);
        };
        serialize(root);
        return oss.str();
    }

    void fromString(const std::string& str) {
        std::istringstream iss(str);
        std::function<std::unique_ptr<BinaryTreeNode<T>>()> deserialize
        = [&]() {
            std::string value;
            if (!(iss >> value) || value == "null") {
                return std::unique_ptr<BinaryTreeNode<T>>(nullptr);
            }
            T data = T();
            std::istringstream(value) >> data;
            auto node = std::make_unique<BinaryTreeNode<T>>(data);
            node->left = deserialize();
            node->right = deserialize();
            return node;
        };
        root = deserialize();
    }

    // --- Level Order Pointer List Creation ---
    LevelOrderPointerList<T> createLevelOrderPointerList() {
        LevelOrderPointerList<T> list;
        if (!root) return list;
        std::queue<BinaryTreeNode<T>*> q;
        q.push(root.get());
        while (!q.empty()) {
            BinaryTreeNode<T>* node = q.front(); q.pop();
            list.push_back(&(node->data));
            if (node->left) q.push(node->left.get());
            if (node->right) q.push(node->right.get());
        }
        return list;
    }

    // --- Existing Iterators (unchanged) ---
    class InOrderIterator {
        std::stack<BinaryTreeNode<T>*> stk;
        void pushLeft(BinaryTreeNode<T>* node) {
            while (node) {
                stk.push(node);
                node = node->left.get();
            }
        }
    public:
        InOrderIterator(BinaryTreeNode<T>* root) { pushLeft(root); }
        bool hasNext() const { return !stk.empty(); }
        T next() {
            if (!hasNext()) throw std::out_of_range("No more elements in the iterator");
            BinaryTreeNode<T>* node = stk.top(); stk.pop();
            pushLeft(node->right.get());
            return node->data;
        }
    };

    InOrderIterator inOrderBegin() { return InOrderIterator(root.get()); }

    class PreOrderIterator {
        std::stack<BinaryTreeNode<T>*> stk;
    public:
        PreOrderIterator(BinaryTreeNode<T>* root) { if (root) stk.push(root); }
        bool hasNext() const { return !stk.empty(); }
        T next() {
            if (!hasNext()) throw std::out_of_range("No more elements in the iterator");
            BinaryTreeNode<T>* node = stk.top(); stk.pop();
            if (node->right) stk.push(node->right.get());
            if (node->left) stk.push(node->left.get());
            return node->data;
        }
    };

    PreOrderIterator preOrderBegin() { return PreOrderIterator(root.get()); }

    class PostOrderIterator {
        std::stack<BinaryTreeNode<T>*> stk;
        BinaryTreeNode<T>* lastVisited = nullptr;
        void pushLeftMost(BinaryTreeNode<T>* node) {
            while (node) {
                stk.push(node);
                if (node->left) node = node->left.get();
                else node = node->right.get();
            }
        }
    public:
        PostOrderIterator(BinaryTreeNode<T>* root) { pushLeftMost(root); }
        bool hasNext() const { return !stk.empty(); }
        T next() {
            if (!hasNext()) throw std::out_of_range("No more elements in the iterator");
            BinaryTreeNode<T>* node = stk.top(); stk.pop();
            T result = node->data;
            if (!stk.empty()) {
                BinaryTreeNode<T>* top = stk.top();
                if (node == top->left.get() && top->right) pushLeftMost(top->right.get());
            }
            return result;
        }
    };

    PostOrderIterator postOrderBegin() { return PostOrderIterator(root.get()); }

    class LevelOrderIterator {
        std::queue<BinaryTreeNode<T>*> q;
    public:
        LevelOrderIterator(BinaryTreeNode<T>* root) { if (root) q.push(root); }
        bool hasNext() const { return !q.empty(); }
        T next() {
            if (!hasNext()) throw std::out_of_range("No more elements in the iterator");
            BinaryTreeNode<T>* node = q.front(); q.pop();
            if (node->left) q.push(node->left.get());
            if (node->right) q.push(node->right.get());
            return node->data;
        }
    };

    LevelOrderIterator levelOrderBegin() { return LevelOrderIterator(root.get()); }
};

int main() {
    BinaryTree<int> tree;
    tree.insert(1);
    tree.insert(2);
    tree.insert(3);
    tree.insert(4);
    tree.insert(5);
    tree.insert(6);
    tree.insert(7);

    std::cout << "In-order traversal: ";
    for (auto it = tree.inOrderBegin(); it.hasNext(); ) {
        std::cout << it.next() << " ";
    }
    std::cout << std::endl;

    std::cout << "Pre-order traversal: ";
    for (auto it = tree.preOrderBegin(); it.hasNext(); ) {
        std::cout << it.next() << " ";
    }
    std::cout << std::endl;

    std::cout << "Post-order traversal: ";
    for (auto it = tree.postOrderBegin(); it.hasNext(); ) {
        std::cout << it.next() << " ";
    }
    std::cout << std::endl;

    std::cout << "Level-order traversal: ";
    for (auto it = tree.levelOrderBegin(); it.hasNext(); ) {
        std::cout << it.next() << " ";
    }
    std::cout << std::endl;

    // --- Level Order Pointer List Usage ---
    auto pointerList = tree.createLevelOrderPointerList();

    std::cout << "Level-order pointer list (forward): ";
    for (auto it = pointerList.begin(); it.valid(); ++it) {
        std::cout << **it << " ";
    }
    std::cout << std::endl;

    std::cout << "Level-order pointer list (backward): ";
    for (auto it = pointerList.rbegin(); it.valid(); --it) {
        std::cout << **it << " ";
    }
    std::cout << std::endl;

    // Example: Save a pointer and increment/decrement
    auto it = pointerList.begin();
    if (it.valid()) {
        std::cout << "First value: " << **it << std::endl;
        ++it;
        if (it.valid()) std::cout << "Second value: " << **it << std::endl;
        --it;
        if (it.valid()) std::cout << "Back to first value: " << **it << std::endl;
    }

    return 0;
}

