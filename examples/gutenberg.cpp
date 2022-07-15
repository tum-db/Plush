#include "../src/hashtable/Hashtable.h"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <span>
#include <unordered_map>


int main(int argc, char** argv) {
    // Dataset can be found here: https://github.com/aparrish/gutenberg-dammit
    Hashtable<std::span<const std::byte>, std::span<const std::byte>, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);

    std::string path("/home/vogel/gutenberg-dammit-files");
    std::string ext(".txt");

    int counter = 0;
    int read_counter = 0;

    std::unordered_map<std::string, std::string> strings;

    std::cout << "Preloading..." << std::endl;

    // We naively preload all books into memory - note that this is the part that takes the longest
    for (auto &p : std::filesystem::recursive_directory_iterator(path))
    {
        if (p.path().extension() == ext) {
            std::ifstream t(p.path());
            std::string str((std::istreambuf_iterator<char>(t)),
                            std::istreambuf_iterator<char>());
            strings[p.path().string()] = str;
        }
        read_counter++;

        if (read_counter % 100 == 0) {
            std::cout << read_counter << std::endl;
        }
    }

    std::cout << "Inserting..." << std::endl;

    std::chrono::time_point<std::chrono::high_resolution_clock> start = std::chrono::high_resolution_clock::now();
    for (auto& pair: strings) {
        std::string key = pair.first;
        table.insert(std::span<std::byte>(reinterpret_cast<std::byte*>(key.data()), key.size()),
                     std::span<std::byte>(reinterpret_cast<std::byte*>(pair.second.data()), 65536));
        if (counter % 100 == 0) {
            std::cout << key << std::endl;
            std::cout << counter << std::endl;
        }
        ++counter;
    }
    std::chrono::time_point<std::chrono::high_resolution_clock> end = std::chrono::high_resolution_clock::now();
    uint64_t curr_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "========== Insert time: " << (curr_ms * 1.0) / 1000  << " s ==========" << std::endl;

    std::unique_ptr<uint8_t[]> content = std::make_unique<uint8_t[]>(10e6);

    table.count();

    std::string key = path + "/052/05206.txt";


    bool found = table.lookup(std::span<std::byte>(reinterpret_cast<std::byte*>(key.data()), key.size()), content.get());

    std::cout << found << std::endl;

    std::cout << content << std::endl;


    return 0;
}
