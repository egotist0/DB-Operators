
#include "operators/operators.h"

#include "common/macros.h"

#define UNUSED(p) ((void)(p))
namespace buzzdb {
namespace operators {

Register Register::from_int(int64_t value) {
  Register r{};
  r.intVal = value;
  return r;
}

Register Register::from_string(const std::string& value) {
  Register r{};
  r.strVal = value;
  return r;
}

Register::Type Register::get_type() const {
  if (this->intVal) return Type::INT64;
  return Type::CHAR16;
}

int64_t Register::as_int() const {
  if (this->intVal) return *this->intVal;
  return 0;
}

std::string Register::as_string() const {
  if (this->strVal) return *this->strVal;
  return std::string{};
}

uint64_t Register::get_hash() const {
  if (this->get_type() == Type::INT64)
    return std::hash<int64_t>{}(*this->intVal);
  return std::hash<std::string>{}(*this->strVal);
}

bool operator==(const Register& r1, const Register& r2) {
  return r1.get_hash() == r2.get_hash();
}

bool operator!=(const Register& r1, const Register& r2) {
  return r1.get_hash() != r2.get_hash();
}

bool operator<(const Register& r1, const Register& r2) {
  assert(r1.get_type() == r2.get_type());
  return (r1.get_type() == Register::Type::INT64)
             ? r1.as_int() < r2.as_int()
             : r1.as_string().compare(r2.as_string()) < 0;
}

bool operator<=(const Register& r1, const Register& r2) {
  assert(r1.get_type() == r2.get_type());
  return (r1.get_type() == Register::Type::INT64)
             ? r1.as_int() <= r2.as_int()
             : r1.as_string().compare(r2.as_string()) <= 0;
}

bool operator>(const Register& r1, const Register& r2) {
  assert(r1.get_type() == r2.get_type());
  return (r1.get_type() == Register::Type::INT64)
             ? r1.as_int() > r2.as_int()
             : r1.as_string().compare(r2.as_string()) > 0;
}

bool operator>=(const Register& r1, const Register& r2) {
  assert(r1.get_type() == r2.get_type());
  return (r1.get_type() == Register::Type::INT64)
             ? r1.as_int() >= r2.as_int()
             : r1.as_string().compare(r2.as_string()) >= 0;
}

Print::Print(Operator& input, std::ostream& stream)
    : UnaryOperator(input), stream(&stream) {}

Print::~Print() = default;

void Print::open() { this->input->open(); }

bool Print::next() {
  if (this->input->next()) {
    std::vector<Register*> regs = this->input->get_output();
    std::string str;

    for (const auto& reg : regs)
      str += (reg->get_type() == Register::Type::INT64)
                 ? (std::to_string(reg->as_int()) + ",")
                 : (reg->as_string() + ",");

    if (regs.size()) {
      str = str.substr(0, str.size() - 1) + '\n';
      *this->stream << str;
    }
    return true;
  }

  return false;
}

void Print::close() { this->input->close(); }

std::vector<Register*> Print::get_output() {
  // Print has no output
  return {};
}

Projection::Projection(Operator& input, std::vector<size_t> attr_indexes)
    : UnaryOperator(input), attr_indexes(std::move(attr_indexes)) {}

Projection::~Projection() = default;

void Projection::open() { this->input->open(); }

bool Projection::next() {
  if (this->input->next()) {
    std::vector<Register*> regs = this->input->get_output();
    for (const auto& attr_index : this->attr_indexes)
      this->output_regs.emplace_back(*regs[attr_index]);
    return true;
  }

  return false;
}

void Projection::close() { this->input->close(); }

std::vector<Register*> Projection::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  this->output_regs.clear();
  return output;
}

Select::Select(Operator& input, PredicateAttributeInt64 predicate)
    : UnaryOperator(input),
      intPredicate(predicate),
      predicateAttribute(PrecidateAttribute::INT) {}

Select::Select(Operator& input, PredicateAttributeChar16 predicate)
    : UnaryOperator(input),
      charPredicate(std::move(predicate)),
      predicateAttribute(Select::PrecidateAttribute::CHAR) {}

Select::Select(Operator& input, PredicateAttributeAttribute predicate)
    : UnaryOperator(input),
      attributePredicate(predicate),
      predicateAttribute(Select::PrecidateAttribute::ATTRIBUTE) {}

Select::~Select() = default;

void Select::open() { this->input->open(); }

bool Select::next() {
  if (this->input->next()) {
    this->output_regs.clear();
    std::vector<Register*> regs = this->input->get_output();

    switch (this->predicateAttribute) {
      case PrecidateAttribute::INT: {
        auto& intReg = regs[this->intPredicate.attr_index];
        Register int_reg = Register::from_int(this->intPredicate.constant);

        switch (this->intPredicate.predicate_type) {
          case Select::PredicateType::EQ:
            if (*intReg == int_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::NE:
            if (*intReg != int_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::LT:
            if (*intReg < int_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::LE:
            if (*intReg <= int_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::GT:
            if (*intReg > int_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::GE:
            if (*intReg >= int_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;
        }

        break;
      }

      case PrecidateAttribute::CHAR: {
        auto& stringReg = regs[this->charPredicate.attr_index];
        Register str_reg = Register::from_string(this->charPredicate.constant);

        switch (this->charPredicate.predicate_type) {
          case Select::PredicateType::EQ:
            if (*stringReg == str_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::NE:
            if (*stringReg != str_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::LT:
            if (*stringReg < str_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::LE:
            if (*stringReg <= str_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::GT:
            if (*stringReg > str_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::GE:
            if (*stringReg >= str_reg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;
        }

        break;
      }

      case PrecidateAttribute::ATTRIBUTE: {
        Register *leftReg = regs[this->attributePredicate.attr_left_index],
                 *rightReg = regs[this->attributePredicate.attr_right_index];

        switch (this->attributePredicate.predicate_type) {
          case Select::PredicateType::EQ:
            if (*leftReg == *rightReg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::NE:
            if (*leftReg != *rightReg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::LT:
            if (*leftReg < *rightReg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::LE:
            if (*leftReg <= *rightReg)
              for (auto& r : regs) this->output_regs.emplace_back(*r);
            return true;

          case Select::PredicateType::GT:
            if (*leftReg > *rightReg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;

          case Select::PredicateType::GE:
            if (*leftReg >= *rightReg)
              for (const auto& reg : regs) this->output_regs.emplace_back(*reg);
            return true;
        }
      }
    }
  }

  return false;
}

void Select::close() { this->input->close(); }

std::vector<Register*> Select::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  return output;
}

Sort::Sort(Operator& input, std::vector<Criterion> criteria)
    : UnaryOperator(input), criteria(std::move(criteria)) {}

Sort::~Sort() = default;

void Sort::open() { this->input->open(); }

bool Sort::next() {
  this->output_regs.clear();

  if (!this->isFinished) {
    while (this->input->next()) {
      std::vector<Register> output_regs;
      std::vector<Register*> regs = this->input->get_output();
      for (const auto& reg : regs) output_regs.emplace_back(*reg);
      this->registers.emplace_back(output_regs);
    }

    for (const auto& c : this->criteria) {
      if (c.desc)
        std::sort(this->registers.begin(), this->registers.end(),
                  [&](const std::vector<Register>& regs1,
                      const std::vector<Register>& regs2) {
                    return regs1[c.attr_index] > regs2[c.attr_index];
                  });
    }

    this->isFinished = true;
  }

  if (this->current_index < this->registers.size()) {
    auto regs = this->registers[this->current_index];
    for (const auto& r : regs) this->output_regs.emplace_back(r);
    this->current_index++;
    return true;
  }

  return false;
}

std::vector<Register*> Sort::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  return output;
}

void Sort::close() { this->input->close(); }

HashJoin::HashJoin(Operator& input_left, Operator& input_right,
                   size_t attr_index_left, size_t attr_index_right)
    : BinaryOperator(input_left, input_right),
      attr_index_left(attr_index_left),
      attr_index_right(attr_index_right) {
  this->input_left = &input_left;
  this->input_right = &input_right;
}

HashJoin::~HashJoin() = default;

void HashJoin::open() {
  input_left->open();
  input_right->open();

  while (input_left->next()) {
    std::vector<Register*> inputs = input_left->get_output();
    std::vector<Register> registers;
    for (const auto* reg : inputs) registers.emplace_back(*reg);
    std::pair<Register, std::vector<Register>> pair =
        std::make_pair(registers.at(attr_index_left), registers);

    regs_map[registers.at(attr_index_left)] = registers;
  }
}

bool HashJoin::next() {
  while (input_right->next()) {
    std::vector<Register> inputs;
    for (Register* reg : input_right->get_output()) inputs.emplace_back(*reg);

    if (regs_map.count(inputs.at(attr_index_right))) {
      std::vector<Register> leftTuple = (regs_map[inputs.at(attr_index_right)]);
      for (size_t i = 0; i < inputs.size(); i++)
        leftTuple.emplace_back(inputs.at(i));

      output_regs = leftTuple;
      return true;
    }
  }

  return false;
}

void HashJoin::close() {
  this->input_left->close();
  this->input_right->close();
}

std::vector<Register*> HashJoin::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  return output;
}

HashAggregation::HashAggregation(Operator& input,
                                 std::vector<size_t> group_by_attrs,
                                 std::vector<AggrFunc> aggr_funcs)
    : UnaryOperator(input),
      group_by_attrs(std::move(group_by_attrs)),
      aggr_funcs(std::move(aggr_funcs)) {}

HashAggregation::~HashAggregation() = default;

void HashAggregation::open() { this->input->open(); }

bool HashAggregation::next() {
  this->output_regs.clear();
  std::unordered_map<Register, int, RegisterHasher> countMap, sumMap;
  std::experimental::optional<Register> minRegister, maxRegister;

  if (!this->isFinished) {
    while (this->input->next()) {
      std::vector<Register*> regs = this->input->get_output();
      for (const auto& aggr_func : this->aggr_funcs) {
        switch (aggr_func.func) {
          case AggrFunc::MAX:
            if (!maxRegister || *regs[aggr_func.attr_index] > *maxRegister)
              maxRegister = *regs[aggr_func.attr_index];
            break;

          case AggrFunc::MIN:
            if (!minRegister || *regs[aggr_func.attr_index] < *minRegister)
              minRegister = *regs[aggr_func.attr_index];
            break;

          case AggrFunc::SUM:
            for (const auto& attr : this->group_by_attrs) {
              Register r = *regs[aggr_func.attr_index], myreg = *regs[attr];
              if (sumMap.find(myreg) != sumMap.end())
                sumMap[myreg] += static_cast<int>(r.as_int());
              else
                sumMap.insert({myreg, static_cast<int>(r.as_int())});
            }
            break;

          case AggrFunc::COUNT:
            for (const auto& attr : this->group_by_attrs)
              countMap[*regs[attr]]++;
        }
      }
    }

    if (sumMap.size()) {
      std::vector<Register> keys;
      for (const auto& it : sumMap) keys.emplace_back(it.first);
      this->numberOfKeys = static_cast<int>(keys.size());
      std::sort(keys.begin(), keys.end());

      for (const auto& key : keys) {
        std::vector<Register> reg_vector;
        reg_vector.emplace_back(key);
        auto sumRegister = Register::from_int(sumMap[key]);
        reg_vector.emplace_back(sumRegister);
        auto countRegister = Register::from_int(countMap[key]);
        reg_vector.emplace_back(countRegister);
        this->temp_sumcount_registers.emplace_back(reg_vector);
      }
    }

    this->isFinished = true;
  }

  if (minRegister) {
    this->output_regs.emplace_back(*minRegister);
    if (maxRegister) this->output_regs.emplace_back(*maxRegister);
    return true;
  }

  if (this->counter < this->numberOfKeys) {
    this->output_regs = this->temp_sumcount_registers[this->counter];
    this->counter++;
    return true;
  }

  return false;
};

void HashAggregation::close() { this->input->close(); }

std::vector<Register*> HashAggregation::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  return output;
}

Union::Union(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

Union::~Union() = default;

void Union::open() {
  this->input_left->open();
  this->input_right->open();
}

bool Union::next() {
  std::unordered_map<Register, int, RegisterHasher> registers_map;

  if (!this->isFinished) {
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (const auto& reg : regs) registers_map[*reg]++;
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (const auto& reg : regs) registers_map[*reg]++;
    }

    for (const auto& reg : registers_map)
      this->registers.emplace_back(reg.first);

    std::sort(this->registers.begin(), this->registers.end(),
              [&](const Register& a, const Register& b) { return a < b; });
    this->isFinished = true;
  }

  if (this->counter < static_cast<int>(this->registers.size())) {
    this->output_regs.emplace_back(this->registers[this->counter]);
    this->counter++;
    return true;
  }

  return false;
}

std::vector<Register*> Union::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  this->output_regs.clear();
  return output;
}

void Union::close() {
  this->input_left->close();
  this->input_right->close();
}

UnionAll::UnionAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

UnionAll::~UnionAll() = default;

void UnionAll::open() {
  this->input_left->open();
  this->input_right->open();
}

bool UnionAll::next() {
  std::unordered_map<Register, int, RegisterHasher> registers_map;

  if (!this->isFinished) {
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (const auto& reg : regs) registers_map[*reg]++;
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (const auto& reg : regs) registers_map[*reg]++;
    }

    for (const auto& reg : registers_map)
      for (int i = 0; i < reg.second; i++)
        this->registers.emplace_back(reg.first);

    std::sort(this->registers.begin(), this->registers.end(),
              [&](const Register& a, const Register& b) { return a < b; });
    this->isFinished = true;
  }

  if (this->counter < static_cast<int>(this->registers.size())) {
    this->output_regs.emplace_back(this->registers[this->counter]);
    this->counter++;
    return true;
  }

  return false;
}

std::vector<Register*> UnionAll::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  this->output_regs.clear();
  return output;
}

void UnionAll::close() {
  this->input_left->close();
  this->input_right->close();
}

Intersect::Intersect(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

Intersect::~Intersect() = default;

void Intersect::open() {
  this->input_left->open();
  this->input_right->open();
}

bool Intersect::next() {
  std::unordered_map<Register, int, RegisterHasher> left_registers,
      right_registers;

  if (!this->isFinished) {
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (const auto& reg : regs) left_registers[*reg]++;
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (const auto& reg : regs) right_registers[*reg]++;
    }

    for (const auto& reg : left_registers)
      if (right_registers.find(reg.first) != right_registers.end())
        this->registers.emplace_back(reg.first);

    std::sort(this->registers.begin(), this->registers.end(),
              [&](const Register& a, const Register& b) { return a < b; });
    this->isFinished = true;
  }

  if (this->counter < static_cast<int>(this->registers.size())) {
    this->output_regs.emplace_back(this->registers[this->counter]);
    this->counter++;
    return true;
  }

  return false;
}

std::vector<Register*> Intersect::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  this->output_regs.clear();
  return output;
}

void Intersect::close() {
  this->input_left->close();
  this->input_right->close();
}

IntersectAll::IntersectAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

IntersectAll::~IntersectAll() = default;

void IntersectAll::open() {
  this->input_left->open();
  this->input_right->open();
}

bool IntersectAll::next() {
  std::unordered_map<Register, int, RegisterHasher> left_registers,
      right_registers;

  if (!this->isFinished) {
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (const auto& reg : regs) left_registers[*reg]++;
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (const auto& reg : regs) right_registers[*reg]++;
    }

    for (const auto& reg : left_registers) {
      auto find = right_registers.find(reg.first);
      if (find != right_registers.end()) {
        auto leftCount = reg.second, rightCount = find->second;
        if (leftCount <= rightCount)
          for (int i = 0; i < leftCount; i++)
            this->registers.emplace_back(reg.first);
        else
          for (int i = 0; i < rightCount; i++)
            this->registers.emplace_back(find->first);
      }
    }

    std::sort(this->registers.begin(), this->registers.end(),
              [&](const Register& a, const Register& b) { return a < b; });
    this->isFinished = true;
  }

  if (this->counter < static_cast<int>(this->registers.size())) {
    this->output_regs.emplace_back(this->registers[this->counter]);
    this->counter++;
    return true;
  }

  return false;
}

std::vector<Register*> IntersectAll::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  this->output_regs.clear();
  return output;
}

void IntersectAll::close() {
  this->input_left->close();
  this->input_right->close();
}

Except::Except(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

Except::~Except() = default;

void Except::open() {
  this->input_left->open();
  this->input_right->open();
}

bool Except::next() {
  std::unordered_map<Register, int, RegisterHasher> left_registers,
      right_registers;

  if (!this->isFinished) {
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (const auto& reg : regs) left_registers[*reg]++;
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (const auto& reg : regs) right_registers[*reg]++;
    }

    for (const auto& reg : left_registers)
      if (right_registers.find(reg.first) == right_registers.end())
        this->registers.emplace_back(reg.first);

    std::sort(this->registers.begin(), this->registers.end(),
              [&](const Register& a, const Register& b) { return a < b; });
    this->isFinished = true;
  }

  if (this->counter < static_cast<int>(this->registers.size())) {
    this->output_regs.emplace_back(this->registers[this->counter]);
    this->counter++;
    return true;
  }

  return false;
}

std::vector<Register*> Except::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  this->output_regs.clear();
  return output;
}

void Except::close() {
  this->input_left->close();
  this->input_right->close();
}

ExceptAll::ExceptAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

ExceptAll::~ExceptAll() = default;

void ExceptAll::open() {
  this->input_left->open();
  this->input_right->open();
}

bool ExceptAll::next() {
  std::unordered_map<Register, int, RegisterHasher> left_registers,
      right_registers;

  if (!this->isFinished) {
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (const auto& reg : regs) left_registers[*reg]++;
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (const auto& reg : regs) right_registers[*reg]++;
    }

    for (const auto& reg : left_registers) {
      auto find = right_registers.find(reg.first);
      if (find != right_registers.end()) {
        int leftCount = reg.second, rightCount = find->second;
        if (leftCount > rightCount)
          for (int i = rightCount; i < leftCount; i++)
            this->registers.emplace_back(reg.first);
      } else {
        int leftCount = reg.second;
        for (int i = 0; i < leftCount; i++)
          this->registers.emplace_back(reg.first);
      }
    }

    std::sort(this->registers.begin(), this->registers.end(),
              [&](const Register& a, const Register& b) { return a < b; });
    this->isFinished = true;
  }

  if (this->counter < static_cast<int>(this->registers.size())) {
    this->output_regs.emplace_back(this->registers[this->counter]);
    this->counter++;
    return true;
  }

  return false;
}

std::vector<Register*> ExceptAll::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_regs) output.emplace_back(&reg);
  this->output_regs.clear();
  return output;
}

void ExceptAll::close() {
  this->input_left->close();
  this->input_right->close();
}

}  // namespace operators
}  // namespace buzzdb
