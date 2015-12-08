//
//  DataManager.swift
//  DUI
//
//  Created by Kota Nakano on 12/8/15.

import CoreData
public class DataManager
{
	public static var logfile: NSFileHandle = NSFileHandle(fileDescriptor: 2)
	private static let privContext: NSManagedObjectContext = {
		let bundles: [NSBundle] = [NSBundle.mainBundle(), NSBundle(forClass: DataManager.self)]
		let context: NSManagedObjectContext = NSManagedObjectContext(concurrencyType: NSManagedObjectContextConcurrencyType.PrivateQueueConcurrencyType)
		if let
			name: String = bundles.first?.bundleIdentifier,
			base: NSURL = bundles.first?.resourceURL,
//			base: NSURL = NSFileManager.defaultManager().URLsForDirectory(NSSearchPathDirectory.DocumentDirectory, inDomains: NSSearchPathDomainMask.UserDomainMask).first,
			sqlurl: NSURL = NSURL(string: "\(name).sqlite", relativeToURL: base),
			model: NSManagedObjectModel = NSManagedObjectModel.mergedModelFromBundles(bundles)
		{
			context.persistentStoreCoordinator = NSPersistentStoreCoordinator(managedObjectModel: model)
			do {
				try context.persistentStoreCoordinator?.addPersistentStoreWithType(NSSQLiteStoreType, configuration: nil, URL: sqlurl, options: [
						NSMigratePersistentStoresAutomaticallyOption: true,
						NSInferMappingModelAutomaticallyOption: true,
//						NSPersistentStoreUbiquitousContentNameKey: name,
//						NSPersistentStoreRebuildFromUbiquitousContentOption: true
					])
			} catch let error as NSError {
				DataManager.log("init", detail: error.description)
			}
		}
		return context
	}()
	
	private static func log(let sender: String, let detail: String) {
		if let data: NSData = "\(NSDate())\t\(sender)\t\(detail)\r\n".dataUsingEncoding(NSUTF8StringEncoding) {
			logfile.seekToEndOfFile()
			logfile.writeData(data)
		}
	}
	
	class var mainContext: NSManagedObjectContext {
		let context: NSManagedObjectContext = NSManagedObjectContext(concurrencyType: .MainQueueConcurrencyType)
		context.parentContext = privContext
		return context
	}
	
	private let context: NSManagedObjectContext
	public init(let thread: NSManagedObjectContextConcurrencyType = .PrivateQueueConcurrencyType, let parent: NSManagedObjectContext = DataManager.privContext) {
		context = NSManagedObjectContext(concurrencyType: thread)
		context.parentContext = parent
	}
	public var subManager: DataManager {
		return DataManager(thread: .PrivateQueueConcurrencyType, parent: context)
	}
	public func store(let complete: ((NSError?)->Void)? = nil) -> DataManager {
		func persistent(let context: NSManagedObjectContext) {
			context.performBlock {
				do {
					try context.save()
					if let context = context.parentContext {
						persistent(context)
					}
					else {
						complete?(nil)
					}
				} catch let error as NSError {
					if let complete = complete {
						complete(error)
					} else {
						DataManager.log("store", detail: error.description)
					}
				}
			}
		}
		persistent(context)
		return self
	}
	public func save(let complete: ((NSError?)->())? = nil) -> DataManager {
		async {
			do {
				try self.context.save()
				complete?(nil)
			} catch let error as NSError {
				if let complete = complete {
					complete(error)
				} else {
					DataManager.log("save", detail: error.description)
				}
			}
		}
		return self
	}
	public func rollback(let complete: (()->())? = nil) -> DataManager {
		async {
			self.context.rollback()
			complete?()
		}
		return self
	}
	public func reset(let complete: (()->())? = nil) -> DataManager {
		async {
			self.context.reset()
			complete?()
		}
		return self
	}
	public func refresh(let object: NSManagedObject? = nil, let complete: (()->())? = nil) -> DataManager {
		async {
			if let object = object {
				self.context.refreshObject(object, mergeChanges: true)
				complete?()
			} else {
				self.context.refreshAllObjects()
				complete?()
			}
		}
		return self
	}
	public func undo(let complete: (()->())? = nil) -> DataManager {
		async {
			self.context.undo()
			complete?()
		}
		return self
	}
	public func redo(let complete: (()->())? = nil) -> DataManager {
		async {
			self.context.redo()
			complete?()
		}
		return self
	}
	public func insert<T: NSManagedObject>(let attribute: [String: AnyObject] = [:]) -> T? {
		var result: T?
		sync {
			if let
				entity: String = NSStringFromClass(T.classForCoder()).componentsSeparatedByString(".").last,
				inserted = NSEntityDescription.insertNewObjectForEntityForName(entity, inManagedObjectContext: self.context)as?T
			{
				inserted.setValuesForKeysWithDictionary(attribute)
				result = inserted
			}
		}
		return result
	}
	public func assign<T: NSManagedObject>(let object: T) -> T? {
		return assign(object.objectID)
	}
	public func assign<T: NSManagedObject>(let objectID: NSManagedObjectID) -> T? {
		var result: T?
		sync {
			do {
				result = try self.context.existingObjectWithID(objectID) as? T
			} catch let error as NSError {
				DataManager.log("assign", detail: error.description)
			}
		}
		return result
	}
	public func assign<T: NSManagedObject>(let attribute: [String: AnyObject] = [:]) -> [T] {
		var result: [T] = []
		if !attribute.isEmpty {
			result = fetch(attribute)
		}
		if result.isEmpty, let inserted: T = insert(attribute) {
			result.append(inserted)
		}
		return result
	}
	public func fetch<T: NSManagedObject>(let attribute: [String: AnyObject?] = [:], let sort: [String: Bool] = [:], let batch: Int = 0, let limit: Int = 0, let offset: Int = 0, let fault: Bool = true, let distinct: Bool = false) -> [T] {
		var result: [T] = []
		sync {
			if let
				entity: String = NSStringFromClass(T.classForCoder()).componentsSeparatedByString(".").last,
				fetched = (try?self.context.executeFetchRequest({
					let request: NSFetchRequest = NSFetchRequest(entityName: $0)
					request.returnsObjectsAsFaults = fault
					request.returnsDistinctResults = distinct
					if 0 < offset {
						request.fetchOffset = offset
					}
					if 0 < limit {
						request.fetchLimit = limit
					}
					if 0 < batch {
						request.fetchBatchSize = batch
					}
					if !$1.isEmpty
					{
						var keys:[String] = []
						var vals:[AnyObject] = []
						for (k, v) in attribute {
							if let v = v {
								if let
									re = try?NSRegularExpression(pattern: "^\\s*(\\w+)\\s*(<|<=|=|=>|>)\\s*$", options: NSRegularExpressionOptions(rawValue: 0)),
									m = re.matchesInString(k, options: NSMatchingOptions(rawValue: 0), range: NSRange(location: 0, length: k.lengthOfBytesUsingEncoding(NSUTF8StringEncoding))).first
								{
									let r1 = k.startIndex.advancedBy(m.rangeAtIndex(1).location) ..< k.startIndex.advancedBy(m.rangeAtIndex(1).location).advancedBy(m.rangeAtIndex(1).length)
									let r2 = k.startIndex.advancedBy(m.rangeAtIndex(2).location) ..< k.startIndex.advancedBy(m.rangeAtIndex(2).location).advancedBy(m.rangeAtIndex(2).length)
									let key: String = k.substringWithRange(r1)
									let cmp: String = k.substringWithRange(r2)
									keys.append("( \(key) \(cmp) %@ )")
								} else {
									keys.append("( \(k) = %@ )")
								}
								vals.append(v)
							} else {
								keys.append("( \(k) = nil )")
							}
						}
						request.predicate = NSPredicate(format: keys.joinWithSeparator(" and "), argumentArray: vals)
					}
					if !$2.isEmpty {
						request.sortDescriptors = $2.map{NSSortDescriptor(key: $0, ascending: $1)}
					}
					return request
					}(entity, attribute, sort)))as?[T]
			{
				result = fetched
			}
		}
		return result
	}
	public func delete(let object: NSManagedObject, let complete: (()->())? = nil) -> DataManager {
		async {
			self.context.deleteObject(object)
			complete?()
		}
		return self
	}
	public func sync(let block:()->()) {
		context.performBlockAndWait(block)
	}
	public func async(let block:()->()) {
		context.performBlock(block)
	}
}
