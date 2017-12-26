#include "qe.h"
#include <cmath>
#include <cstring>
#include <iostream>
using namespace std;


vector<Field> Utility::getFields(const vector<Attribute> recDes, const void* data){

	const int nullBytes = (int) ceil( (float) (recDes.size()) / 8.0 );
	int scanOffset = nullBytes;

	vector<Field> results;
	for (int i = 0; i < recDes.size(); i++){

		unsigned char nb;
		int nbOffset = (int) floor((float) i / 8.0);
		std::memcpy(&nb, data + nbOffset, 1);
		int bitToCheck = i - nbOffset;
		nb = nb << i;
		nb = nb & ((unsigned char) 128);

		Field mfield;
		if(nb == (unsigned char) 0){
			mfield.valid = 1;					// is valid	(NOT NULL)
			mfield.properties = recDes[i];		// set the attribute

			if(mfield.properties.type == TypeInt){
				std::memcpy((char*) &mfield.integer_value, data + scanOffset, 4);
				scanOffset += 4;
			}else if(mfield.properties.type == TypeReal){
				std::memcpy((char*) &mfield.float_value, data + scanOffset, 4);
				scanOffset += 4;
			}else if(mfield.properties.type == TypeVarChar){
				int lov;
				std::memcpy((char*) &lov, data + scanOffset, 4);
				char tk[lov + 1];
				std::memcpy(tk, data + scanOffset + 4, lov);
				tk[lov] = '\0';
				mfield.varchar_value = string(tk);
				mfield.properties.length = lov;

				scanOffset += (4 + lov);
			}

		}else{
			mfield.valid = 0;					// is not valid (NULL)
			mfield.properties = recDes[i];
			scanOffset += 0;					// remains the same
		}

		results.push_back(mfield);

	}

	return results;
}

bool Utility::match(const CompOp op, const Field leftValue, const Field rightValue){
	if(leftValue.properties.type != rightValue.properties.type)
		return false;

	int nb1 = leftValue.valid;
	int nb2 = rightValue.valid;

	if(nb1 == 0){
		if(nb2 == 0){
			if((op == EQ_OP) || (op == NO_OP))
				return true;
			else
				return false;
		}else{
			if((op == NO_OP) || (op == NE_OP))
				return true;
			else
				return false;
		}

	}else{
		if(nb2 == 0){
			if((op == NO_OP) || (op == NE_OP))
				return true;
			else
				return false;
		}
	}


	// ---> WHEN both values are 'not' null

	AttrType mType = leftValue.properties.type;

	int i_lhs, i_rhs;
	float f_lhs, f_rhs;
	string s_lhs, s_rhs;

	if(mType == TypeInt){
		i_lhs = leftValue.integer_value;
		i_rhs = rightValue.integer_value;
	}else if(mType == TypeReal){
		f_lhs = leftValue.float_value;
		f_rhs = rightValue.float_value;
	}else{
		s_lhs = leftValue.varchar_value;
		s_rhs = rightValue.varchar_value;
	}

	switch(op){
		case EQ_OP:
			if(mType == TypeInt)
				if(i_lhs == i_rhs)
					return true;
				else
					return false;
			else if(mType == TypeReal)
				if(f_lhs == f_rhs)
					return true;
				else
					return false;
			else if(mType == TypeVarChar)
				if(s_lhs == s_rhs)
					return true;
				else
					return false;
			break;
		case LT_OP:
			if(mType == TypeInt)
				if(i_lhs < i_rhs)
					return true;
				else
					return false;
			else if(mType == TypeReal)
				if(f_lhs < f_rhs)
					return true;
				else
					return false;
			else if(mType == TypeVarChar)
				if(s_lhs < s_rhs)
					return true;
				else
					return false;
			break;
		case LE_OP:
			if(mType == TypeInt)
				if(i_lhs <= i_rhs)
					return true;
				else
					return false;
			else if(mType == TypeReal)
				if(f_lhs <= f_rhs)
					return true;
				else
					return false;
			else if(mType == TypeVarChar)
				if(s_lhs <= s_rhs)
					return true;
				else
					return false;
			break;
		case GT_OP:
			if(mType == TypeInt)
				if(i_lhs > i_rhs)
					return true;
				else
					return false;
			else if(mType == TypeReal)
				if(f_lhs > f_rhs)
					return true;
				else
					return false;
			else if(mType == TypeVarChar)
				if(s_lhs > s_rhs)
					return true;
				else
					return false;
			break;
		case GE_OP:
			if(mType == TypeInt)
				if(i_lhs >= i_rhs)
					return true;
				else
					return false;
			else if(mType == TypeReal)
				if(f_lhs >= f_rhs)
					return true;
				else
					return false;
			else if(mType == TypeVarChar)
				if(s_lhs >= s_rhs)
					return true;
				else
					return false;
			break;
		case NE_OP:
			if(mType == TypeInt)
				if(i_lhs != i_rhs)
					return true;
				else
					return false;
			else if(mType == TypeReal)
				if(f_lhs != f_rhs)
					return true;
				else
					return false;
			else if(mType == TypeVarChar)
				if(s_lhs != s_rhs)
					return true;
				else
					return false;
			break;
		case NO_OP:
			return true;
			break;
	}

	return false;
}


int Utility::fetchRecordLength(const void* data, const vector<Attribute> recDes){

	const int nb = (int) ceil( (float) recDes.size()/8.0 );
	unsigned char nullbytes[nb];
	std::memcpy(nullbytes, data, nb);

	int recordlength = nb;
	int scanoffset = nb;

	for (int i = 0; i < recDes.size(); i++){
		unsigned char toconsider = nullbytes[ (int) floor( (float) i/8.0 ) ];
		toconsider = toconsider << i;
		toconsider = toconsider & ((unsigned char) 128);

		if (toconsider == (unsigned char) 128){
			// is null
			continue;
		}else{
			// not null
			if(recDes[i].type != TypeVarChar){
				scanoffset += 4;
				recordlength += 4;
			}else if(recDes[i].type == TypeVarChar){
				int lov;
				std::memcpy((char*) &lov, data + scanoffset, 4);
				scanoffset += (4 + lov);
				recordlength += lov;
			}
		}
	}

	return recordlength;
}


Filter::Filter(Iterator* input, const Condition &condition){
	this->lowerIterator = input;
	this->condition = condition;
	lowerIterator->getAttributes(this->recDes);
}


void Filter::getAttributes(vector<Attribute> &attrs) const{
	//lowerIterator->getAttributes(attrs);
	attrs = this->recDes;
}

RC Filter::getNextTuple(void *data) {

	while(true){


		if(lowerIterator->getNextTuple(data) != (RC) 0)
			return QE_EOF;

		vector<Field> fields = Utility::getFields(recDes, data);

		Field leftField;
		for(int i = 0; i < fields.size(); i++){
			if(fields[i].properties.name == condition.lhsAttr){
				leftField = fields[i];
				break;
			}
		}

		bool match = false;
		if(condition.bRhsIsAttr){
			// RHS is also an attribute
			Field rightField;
			for(int i = 0; i < fields.size(); i++){
				if(fields[i].properties.name == condition.rhsAttr){
					rightField = fields[i];
					break;
				}
			}
			match = Utility::match(condition.op, leftField, rightField);
		}else{
			// RHS is a fixed value
			// create a field from it


			// NO NULL BYTE
			Field rightField;
			if(!condition.rhsValue.data)
				rightField.valid = 0;
			else
				rightField.valid = 1;

			rightField.properties.name = leftField.properties.name;	// doesn't matter though
			rightField.properties.type = leftField.properties.type;

			if(rightField.valid == 1){
				if(rightField.properties.type == TypeInt){
					rightField.properties.length = 4;
					std::memcpy((char*) &rightField.integer_value, condition.rhsValue.data, 4);
				}else if(rightField.properties.type == TypeReal){
					rightField.properties.length = 4;
					std::memcpy((char*) &rightField.float_value, condition.rhsValue.data, 4);
				}else{
					int lov;
					std::memcpy((char*) &lov, condition.rhsValue.data, 4);
					rightField.properties.length = lov;
					char tk[lov + 1];
					std::memcpy(tk, condition.rhsValue.data + 4, lov);
					tk[lov] = '\0';
					rightField.varchar_value = string(tk);
				}
			}

			match = Utility::match(condition.op, leftField, rightField);
		}

		if(match){
			// this is the tuple
			//cout << "Tuple satisfies condition" << endl;
			break;
		}else{
			//cout << "Tuple DOES NOT satisfy condition" << endl;
			continue;
		}
	}


	return (RC) 0;
}


Project::Project(Iterator *input, const vector<string> &attrNames){

	this->lowerIterator = input;
	lowerIterator->getAttributes(this->originalRecDes);

	this->filteredRecDes.clear();
	for(int i = 0; i < attrNames.size(); i++){
		for(int j = 0; j < originalRecDes.size(); j++){
			if (originalRecDes[j].name == attrNames[i]){
				(this->filteredRecDes).push_back(originalRecDes[j]);
				break;
			}
		}
	}


}

void Project::getAttributes(vector<Attribute> &attrs) const{
	attrs = this->filteredRecDes;
}


RC Project::getNextTuple(void *data){

		char lowerData[2000];
		if(lowerIterator->getNextTuple(lowerData) != (RC) 0)
			return QE_EOF;

		vector<Field> fields = Utility::getFields(originalRecDes, lowerData);

		// now start filling data in *data based on filteredRecDes

		const int nullBytes = (int) ceil((float) filteredRecDes.size()/8.0);
		char nb[nullBytes];
		for(int i = 0; i < nullBytes; i++)			// initialize to 00000000000...
			nb[i] = (char) 0;

		int writeOffset = nullBytes;
		int matched = 0;	// just to make sure that lowerdata has all filtered values

		for (int i = 0; i < filteredRecDes.size(); i++){
			for (int j = 0; j < fields.size(); j++){
				if (fields[j].properties.name == filteredRecDes[i].name){
					// record matched
					// insert in *data

					matched++;

					Field mfield = fields[j];
					if (mfield.valid == 0){
						// NULL
						char toChange = nb[(int) floor((float) i/8.0)];
						char mask = 128 >> i;
						toChange |= mask;
						nb[(int) floor((float) i/8.0)] = toChange;
					}else{
						// not null
						if(mfield.properties.type == TypeInt){
							std::memcpy(data + writeOffset, (char*) &mfield.integer_value, 4);
							writeOffset += 4;
						}else if(mfield.properties.type == TypeReal){
							std::memcpy(data + writeOffset, (char*) &mfield.float_value, 4);
							writeOffset += 4;
						}else{
							int lov = mfield.varchar_value.length();
							std::memcpy(data + writeOffset, (char*) &lov, 4);
							std::memcpy(data + writeOffset + 4, mfield.varchar_value.c_str(), lov);
							writeOffset += (4 + lov);
						}
					}

					break;
				}
			}
		}

		if(matched != filteredRecDes.size()){
			return (RC) -1;
		}

		// WRITE NULL BYTES TO *data
		std::memcpy(data, nb, nullBytes);

		return (RC) 0;
}


Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op){
	this->lowerIterator = input;
	this->attrToConsider = aggAttr;			// passed in attribute's name is rel.attr     Ex. employee.age, not age
	this->func = op;
	lowerIterator->getAttributes(this->recDes);
	this->gntCalled = false;
}

RC Aggregate::getNextTuple(void *data){

	if(this->gntCalled == true)
		return QE_EOF;

	bool defined = false;


	float max;		// typecast on demand
	float min;		// typecast on demand
	float count = 0.0;
	float sum;		// typecast on demand

	char tuple[2000];
	while(lowerIterator->getNextTuple(tuple) != -1){

		vector<Field> mfields = Utility::getFields(this->recDes, tuple);
		for(int i = 0; i < mfields.size(); i++){
			if (mfields[i].properties.name == attrToConsider.name){

				Field field = mfields[i];
				if (attrToConsider.type == TypeInt){

					if(!defined){
						if(field.valid == 1){
							defined = true;
							max = (float) field.integer_value;
							min = (float) field.integer_value;
							sum = (float) field.integer_value;
							count = 1.0;
						}
					}else{
						if(field.valid == 1){
							if ((float) field.integer_value > max)
								max = (float) field.integer_value;
							if ((float) field.integer_value < min)
								min = (float) field.integer_value;
							sum += (float) field.integer_value;
							count += 1.0;
						}
					}


				}else if(attrToConsider.type == TypeReal){

					if(!defined){
						if(field.valid == 1){
							defined = true;
							max = (float) field.float_value;
							min = (float) field.float_value;
							sum = (float) field.float_value;
							count = 1.0;
						}
					}else{
						if(field.valid == 1){
							if (field.float_value > max)
								max = (float) field.float_value;
							if (field.float_value < min)
								min = (float) field.float_value;
							sum += (float) field.float_value;
							count += 1.0;
						}
					}

				}


				break;
			}	// condition where field matches

		}	// iterating over all the fields in current tuple

 	}	// iterating over all the tuples

	// Iterated over all tuples
	// Now return


	char nb;
	if(!defined){
		nb = 128;
		std::memcpy(data, &nb, 1);
	}else{
		nb = 0;
		std::memcpy(data, &nb, 1);
		// will be an integer or real value (assumption)

		// Always return in float format
			float avg;
			switch(func){
				case MIN:
					std::memcpy(data + 1, (char*) &min, 4);
					break;
				case MAX:
					std::memcpy(data + 1, (char*) &max, 4);
					break;
				case COUNT:
					std::memcpy(data + 1, (char*) &count, 4);
					break;
				case SUM:
					std::memcpy(data + 1, (char*) &sum, 4);
					break;
				case AVG:
					avg = sum/count;
					std::memcpy(data + 1, (char*) &avg, 4);
					break;
			}

	}

	this->gntCalled = true;
	return (RC) 0;
}


void Aggregate::getAttributes(vector<Attribute> &attrs) const{

	string newName = attrToConsider.name;
	switch(func){
		case MIN:
			newName = "MIN(" + newName + ")";
			break;
		case MAX:
			newName = "MAX(" + newName + ")";
			break;
		case COUNT:
			newName = "COUNT(" + newName + ")";
			break;
		case SUM:
			newName = "SUM(" + newName + ")";
			break;
		case AVG:
			newName = "AVG(" + newName + ")";
			break;
	}
	Attribute toSend = attrToConsider;
	toSend.name = newName;

	attrs.clear();
	attrs.push_back(toSend);
}



INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition){
	this->leftTupleSet = false;
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	leftIn->getAttributes(this->leftRecDes);
	rightIn->getAttributes(this->rightRecDes);

}




RC INLJoin::getNextTuple(void *data){

	while(true){

		// only consider equality operations
		if(condition.op != EQ_OP){
			return (RC) -1;
		}

		// check if left tuple set or not
		if(!leftTupleSet){
			RC result = leftIn->getNextTuple(leftTuple);
			if (result != (RC) 0){
				return QE_EOF;	// no more tuples on left hand side
			}else{
				vector<Field> fields = Utility::getFields(this->leftRecDes, leftTuple);
				for (int i = 0; i < fields.size(); i++){
					if (fields[i].properties.name == condition.lhsAttr){
						Field mfield = fields[i];

						char* key;

						if(mfield.valid == 0){
							leftTupleSet = false;
							continue;
						}else{
							if(mfield.properties.type == TypeInt){
								key = new char[4];
								std::memcpy(key, (char*) &mfield.integer_value, 4);
							}else if(mfield.properties.type == TypeReal){
								key = new char[4];
								std::memcpy(key, (char*) &mfield.float_value, 4);
								//cout << "LHS Value: " << mfield.float_value << endl;
							}else if(mfield.properties.type == TypeVarChar){
								int lov = mfield.varchar_value.length();
								key = new char[4 + lov];
								std::memcpy(key, (char*) &lov, 4);
								std::memcpy(key + 4, mfield.varchar_value.c_str(), lov);
							}
						}

						rightIn->setIterator(key, key, true, true);		// equality condition. *lowKey, *highKey
						delete[] key;
						leftTupleSet = true;
						break;
					}
				}
			}
		}

		// now i have a valid tuple on the LHS and a valid iterator on the RHS (could be an end of file)
		char rightTuple[PAGE_SIZE];
		RC result = rightIn->getNextTuple(rightTuple);

		if(result != (RC) 0){
			// end of file on RHS
			leftTupleSet = false;
			continue;
		}else{

			// NO NEED TO REMOVE THE JOINED COLUMN


			// join the leftTuple and the rightTuple
			const int leftRecordLength = Utility::fetchRecordLength(leftTuple, leftRecDes);
			const int rightRecordLength = Utility::fetchRecordLength(rightTuple, rightRecDes);


			// merge their null bytes and then the actual record
			const int leftnb = (int) ceil( (float) leftRecDes.size() / 8.0);
			const int rightnb = (int) ceil( (float) rightRecDes.size() / 8.0);
			const int mergednb = (int) ceil( (float) (leftRecDes.size() + rightRecDes.size()) / 8.0);

			unsigned char leftNullArray[leftnb];
			unsigned char rightNullArray[rightnb];
			unsigned char mergedNullArray[mergednb];

			std::memcpy(leftNullArray, leftTuple, leftnb);
			std::memcpy(rightNullArray, rightTuple, rightnb);
			for(int i = 0; i < mergednb; i++){
				mergedNullArray[i] = (unsigned char) 0;			// set everyting to 0000000...
			}


			int writingBit = 0;
			for (int i = 0; i < leftRecDes.size(); i++){

				unsigned char toConsider = leftNullArray[(int) floor( (float) i/8.0 )];
				toConsider = toConsider << i;
				toConsider &= (unsigned char) 128;

				if(toConsider == (unsigned char) 128){
					// this bit was set
								// set the writing bit to be 1 in merged array
					unsigned char toConsider = mergedNullArray[(int) floor( (float) writingBit/8.0 )];
					unsigned char setter = ((unsigned char) 128) >> (writingBit - 8*( (int) floor( (float) writingBit/8.0 ) ) );
					toConsider |= setter;
					mergedNullArray[(int) floor( (float) writingBit/8.0 )] = toConsider;
				}else{
					// this was not set // merged null byte already has 0
				}
				writingBit++;
			}


			for (int i = 0; i < rightRecDes.size(); i++){

				unsigned char toConsider = rightNullArray[(int) floor( (float) i/8.0 )];
				toConsider = toConsider << i;
				toConsider &= (unsigned char) 128;

				if(toConsider == (unsigned char) 128){
					// this bit was set
								// set the writing bit to be 1 in merged array
					unsigned char toConsider = mergedNullArray[(int) floor( (float) writingBit/8.0 )];
					unsigned char setter = ((unsigned char) 128) >> (writingBit - 8*( (int) floor( (float) writingBit/8.0 ) ) );
					toConsider |= setter;
					mergedNullArray[(int) floor( (float) writingBit/8.0 )] = toConsider;
				}else{
					// this was not set // merged null byte already has 0
				}
				writingBit++;
			}



			// everything is set now
			std::memcpy(data, mergedNullArray, mergednb);
			std::memcpy(data + mergednb, leftTuple + leftnb, leftRecordLength - leftnb);
			std::memcpy(data + mergednb + leftRecordLength - leftnb, rightTuple + rightnb, rightRecordLength - rightnb);
		}

		return (RC) 0;





	}

}



void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	for(Attribute atr: leftRecDes)
		attrs.push_back(atr);
	for(Attribute atr: rightRecDes)
		attrs.push_back(atr);
}





BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPages = numPages;
	this->blockSet = false;
	this->duplicatePointer = -1;
	leftIn->getAttributes(this->leftRecDes);
	rightIn->getAttributes(this->rightRecDes);
}

bool BNLJoin::matchKeys(const int vectorIndex, const void *rightTuple){

	Attribute leftatr, rightatr;
	AttrType type;

	char* leftkey = hashedVector[vectorIndex].key;			// no need to allocate new space
	if (!leftkey)
		return false;		// NULL != NULL

	for(int i = 0; i < leftRecDes.size(); i++){
		if (leftRecDes[i].name == condition.lhsAttr){
			leftatr = leftRecDes[i];
			break;
		}
	}

	for(int i = 0; i < rightRecDes.size(); i++){
		if (rightRecDes[i].name == condition.rhsAttr){
			rightatr = rightRecDes[i];
			break;
		}
	}

	if(leftatr.type != rightatr.type)
		return false;

	type = leftatr.type;

	// iterate over right tuple
	const int nb = (int) ceil((float) rightRecDes.size()/8.0);
	unsigned char nullbytes[nb];
	std::memcpy(nullbytes, rightTuple, nb);
	int scanoffset = nb;

	for (int i = 0; i < rightRecDes.size(); i++){
		unsigned char toconsider = nullbytes[ (int) floor( (float) i/8.0 )  ];
		toconsider = toconsider << i;
		toconsider = toconsider & ((unsigned char) 128);
		if(toconsider == ((unsigned char) 128)){
			// was null
			if(rightRecDes[i].name == rightatr.name){
				return false;		// NULL != NULL
			}
		}else{
			// not null
			if(rightRecDes[i].name == rightatr.name){
				if(type != TypeVarChar){
					int result = std::memcmp(leftkey, rightTuple + scanoffset, 4);
					if (result == 0)
						return true;
					else
						return false;
				}else{
					int lovl;
					std::memcpy((char*) &lovl, leftkey, 4);
					int lovr;
					std::memcpy((char*) &lovr, rightTuple + scanoffset, 4);
					if (lovl != lovr)
						return false;
					int result = std::memcmp(leftkey, rightTuple + scanoffset, 4 + lovl);
					if (result == 0)
						return true;
					else
						return false;
				}

			}else{
				if(type != TypeVarChar){
					scanoffset += 4;
				}else{
					int lov;
					std::memcpy((char*) &lov, rightTuple + scanoffset, 4);
					scanoffset += (4 + lov);
				}
			}


		}
	}


	return false;
}



RC BNLJoin::setRecordThings(const void *leftTuple, int &keylength, int &recordlength){


	Attribute matr;
	for (int i = 0; i < leftRecDes.size(); i++){
		if(leftRecDes[i].name == condition.lhsAttr){
			matr = leftRecDes[i];
			break;
		}
	}

	recordlength = Utility::fetchRecordLength(leftTuple, leftRecDes);

	const int nb = (int) ceil((float) leftRecDes.size()/8.0);
	unsigned char nullbytes[nb];
	std::memcpy(nullbytes, leftTuple, nb);

	keylength = 0;

	int scanoffset = nb;
	for (int i = 0; i < leftRecDes.size(); i++){

		unsigned char toconsider = nullbytes[ (int) floor( (float) i/8.0 )  ];
		toconsider = toconsider << i;
		toconsider = toconsider & ((unsigned char) 128);
		if(toconsider == ((unsigned char) 128)){
			// was null

			if(leftRecDes[i].name == matr.name){
				// this is the attribute
				keylength = 0;
				HashWrapper hw;
				hw.key = NULL;
				hw.record = new char[recordlength];
				std::memcpy(hw.record, leftTuple, recordlength);
				hashedVector.push_back(hw);
			}

		}else{
			// not null

			if(leftRecDes[i].name == matr.name){
				// this is the attribute
				if(matr.type != TypeVarChar){
					keylength = 4;
					HashWrapper hw;
					hw.key = new char[4];
					std::memcpy(hw.key, leftTuple + scanoffset, 4);
					hw.record = new char[recordlength];
					std::memcpy(hw.record, leftTuple, recordlength);
					hashedVector.push_back(hw);
					scanoffset += 4;
				}else{
					int lov;
					std::memcpy((char*) &lov, leftTuple + scanoffset, 4);
					keylength = 4 + lov;
					HashWrapper hw;
					hw.key = new char[4 + lov];
					std::memcpy(hw.key, leftTuple + scanoffset, 4 + lov);
					hw.record = new char[recordlength];
					std::memcpy(hw.record, leftTuple, recordlength);
					hashedVector.push_back(hw);
					scanoffset += (4 + lov);
				}

			}else{
				if(matr.type != TypeVarChar){
					scanoffset += 4;
				}else{
					int lov;
					std::memcpy((char*) &lov, leftTuple + scanoffset, 4);
					scanoffset += (4 + lov);
				}
			}

		}

	}

	return (RC) 0;
}

RC BNLJoin::releaseMemory(){


	// iterate over vector and release everything
	for (int i = 0; i < hashedVector.size(); i++){


		if (hashedVector[i].key)
			delete[] hashedVector[i].key;
		if (hashedVector[i].record)
			delete[] hashedVector[i].record;
	}

	return (RC) 0;
}


// TODO : Remove recursion
RC BNLJoin::getNextTuple(void *data){

	while(true){
		if(condition.op != EQ_OP){
				cout << "bad condition" << endl;
				return (RC) -1;
			}

			if(!blockSet){
				int filled = 0;
				char temp_left_tuple[PAGE_SIZE];

				//int xx = 0;
				while( (filled < (PAGE_SIZE * numPages)) && (leftIn->getNextTuple(temp_left_tuple) != -1) ){


					int keylength = 0;
					int recordlength = 0;

					// chutiyap in set record things....
					setRecordThings(temp_left_tuple, keylength, recordlength);
					filled += (keylength + recordlength);
				}

				if (filled == 0){
					return (RC) -1;
				}			// EOF on left hand side

				blockSet = true;
				rightIn->setIterator();
				//cout << "Loading new block from LHS" << endl;
			}

			int scanVectorOffset = -1;		// to check if duplicate or not

			if(duplicatePointer == -1){
				RC result = rightIn->getNextTuple(rightTuple);			// load fresh record into righttuple
				if (result != (RC) 0){
					blockSet = false;		// EOF on right side
					releaseMemory();		// new block needs to be loaded
					//cout << "RECUSRRR -------" << endl;
					//return getNextTuple(data);	// recursive call
					continue;
				}
			}else{
				scanVectorOffset = duplicatePointer;
			}


			if(scanVectorOffset == -1){		// get the record from the hashed vector
				for (int i = 0; i < hashedVector.size(); i++){
					if(matchKeys(i, rightTuple)){
						scanVectorOffset = i;
						break;
					}
				}
			}

			if(scanVectorOffset == -1){
				blockSet = true;	// still -1 (no match)..., then just call recursion
				//cout << "RECUSRRR -------" << endl;
				//return getNextTuple(data);
				continue;
			}

			// ************ DUPLICATE CHECK AND SET

			bool newDuplicate = false;
			for (int i = scanVectorOffset + 1; i < hashedVector.size(); i++){
				if(matchKeys(i, rightTuple)){
					newDuplicate = true;
					duplicatePointer = i;
					break;
				}
			}
			if(!newDuplicate)
				duplicatePointer = -1;


			// ************** MERGE

			const int leftRecordLength = Utility::fetchRecordLength(hashedVector[scanVectorOffset].record, leftRecDes);
			const int rightRecordLength = Utility::fetchRecordLength(rightTuple, rightRecDes);

			char leftTuple[leftRecordLength];
			std::memcpy(leftTuple, hashedVector[scanVectorOffset].record, leftRecordLength);

			// merge their null bytes and then the actual record
			const int leftnb = (int) ceil( (float) leftRecDes.size() / 8.0);
			const int rightnb = (int) ceil( (float) rightRecDes.size() / 8.0);
			const int mergednb = (int) ceil( (float) (leftRecDes.size() + rightRecDes.size()) / 8.0);

			unsigned char leftNullArray[leftnb];
			unsigned char rightNullArray[rightnb];
			unsigned char mergedNullArray[mergednb];

			std::memcpy(leftNullArray, leftTuple, leftnb);
			std::memcpy(rightNullArray, rightTuple, rightnb);
			for(int i = 0; i < mergednb; i++){
				mergedNullArray[i] = (unsigned char) 0;			// set everyting to 0000000...
			}


			int writingBit = 0;
			for (int i = 0; i < leftRecDes.size(); i++){

				unsigned char toConsider = leftNullArray[(int) floor( (float) i/8.0 )];
				toConsider = toConsider << i;
				toConsider &= (unsigned char) 128;

				if(toConsider == (unsigned char) 128){
					// this bit was set
								// set the writing bit to be 1 in merged array
					unsigned char toConsider = mergedNullArray[(int) floor( (float) writingBit/8.0 )];
					unsigned char setter = ((unsigned char) 128) >> (writingBit - 8*( (int) floor( (float) writingBit/8.0 ) ) );
					toConsider |= setter;
					mergedNullArray[(int) floor( (float) writingBit/8.0 )] = toConsider;
				}else{
					// this was not set // merged null byte already has 0
				}
				writingBit++;
			}


			for (int i = 0; i < rightRecDes.size(); i++){

				unsigned char toConsider = rightNullArray[(int) floor( (float) i/8.0 )];
				toConsider = toConsider << i;
				toConsider &= (unsigned char) 128;

				if(toConsider == (unsigned char) 128){
					// this bit was set
								// set the writing bit to be 1 in merged array
					unsigned char toConsider = mergedNullArray[(int) floor( (float) writingBit/8.0 )];
					unsigned char setter = ((unsigned char) 128) >> (writingBit - 8*( (int) floor( (float) writingBit/8.0 ) ) );
					toConsider |= setter;
					mergedNullArray[(int) floor( (float) writingBit/8.0 )] = toConsider;
				}else{
					// this was not set // merged null byte already has 0
				}
				writingBit++;
			}

			// everything is set now
			std::memcpy(data, mergedNullArray, mergednb);
			std::memcpy(data + mergednb, leftTuple + leftnb, leftRecordLength - leftnb);
			std::memcpy(data + mergednb + leftRecordLength - leftnb, rightTuple + rightnb, rightRecordLength - rightnb);


			//cout << "merged and returned..." << endl;
			return (RC) 0;
	}


}


void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	for(Attribute atr: leftRecDes)
		attrs.push_back(atr);
	for(Attribute atr: rightRecDes)
		attrs.push_back(atr);
}

